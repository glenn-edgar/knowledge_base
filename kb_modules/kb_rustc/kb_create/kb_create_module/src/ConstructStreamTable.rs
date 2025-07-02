use std::collections::HashMap;
use postgres::{Client, Error, NoTls, types::Json, Row};
use serde_json::Value;
use crate::ConstructKb;

/// Manages a stream table tied to a ConstructKb, setting up schema and
/// synchronizing stream entries based on KB-defined stream fields.
pub struct ConstructStreamTable {
    client: Client,
    construct_kb: ConstructKb,
    database: String,
    table_name: String,
}

impl ConstructStreamTable {
    /// Create a new stream-table manager, dropping/re-creating schema.
    pub fn new(
        mut client: Client,
        construct_kb: ConstructKb,
        database: &str,
    ) -> Result<Self, Error> {
        let table_name = format!("{}_stream", database);
        let mut inst = ConstructStreamTable { client, construct_kb, database: database.into(), table_name };
        inst.setup_schema()?;
        Ok(inst)
    }

    fn setup_schema(&mut self) -> Result<(), Error> {
        // Drop existing
        let drop = format!("DROP TABLE IF EXISTS \"{}\" CASCADE;", self.table_name);
        self.client.batch_execute(&drop)?;

        // Ensure ltree
        self.client.batch_execute("CREATE EXTENSION IF NOT EXISTS ltree;")?;

        // Drop again (optional mirror of Python)
        self.client.batch_execute(&drop)?;

        // Create table
        let create = format!(r#"
            CREATE TABLE "{}" (
                id SERIAL PRIMARY KEY,
                path LTREE,
                recorded_at TIMESTAMPTZ DEFAULT NOW(),
                valid BOOLEAN DEFAULT FALSE,
                data JSONB
            );
        "#, self.table_name);
        self.client.batch_execute(&create)?;

        // Indexes
        let idxs = vec![
            format!("CREATE INDEX IF NOT EXISTS idx_{}_path_gist ON \"{}\" USING GIST(path);", self.table_name, self.table_name),
            format!("CREATE INDEX IF NOT EXISTS idx_{}_path_btree ON \"{}\"(path);", self.table_name, self.table_name),
            format!("CREATE INDEX IF NOT EXISTS idx_{}_recorded_at ON \"{}\"(recorded_at);", self.table_name, self.table_name),
            format!("CREATE INDEX IF NOT EXISTS idx_{}_recorded_at_desc ON \"{}\"(recorded_at DESC);", self.table_name, self.table_name),
            format!("CREATE INDEX IF NOT EXISTS idx_{}_path_recorded_at ON \"{}\"(path, recorded_at);", self.table_name, self.table_name),
        ];
        for sql in idxs { self.client.batch_execute(&sql)?; }
        Ok(())
    }

    /// Defines a new stream field in KB and returns a summary JSON.
    pub fn add_stream_field(
        &mut self,
        stream_key: &str,
        stream_length: i32,
        description: &str,
    ) -> Result<Value, Error> {
        // Add KB info node
        let mut props = HashMap::new();
        props.insert("stream_length".to_string(), Value::Number(stream_length.into()));
        // NOTE: empty data object
        let data = Value::Object(Default::default());
        self.construct_kb.add_info_node(
            "KB_STREAM_FIELD",
            stream_key,
            &mut props,
            &data,
            Some(description),
        )?;

        // Build result JSON
        Ok(json!({
            "stream": "success",
            "message": format!("stream field '{}' added successfully", stream_key),
            "properties": props,
            "data": description,
        }))
    }

    /// Remove all entries whose path matches any in `invalid_paths`, in chunks.
    pub fn remove_invalid_stream_fields(
        &mut self,
        invalid_paths: &[String],
        chunk_size: usize,
    ) -> Result<(), Error> {
        if invalid_paths.is_empty() { return Ok(()); }

        for chunk in invalid_paths.chunks(chunk_size) {
            // Build placeholders $1,$2,...
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
            let in_clause = placeholders.join(",");
            let stmt = format!(
                "DELETE FROM \"{}\" WHERE path IN ({});",
                self.table_name,
                in_clause
            );
            // Convert chunk to &[&(dyn ToSql)]
            let params: Vec<&(dyn postgres::types::ToSql + Sync)> =
                chunk.iter().map(|s| s as &(dyn postgres::types::ToSql + Sync)).collect();
            self.client.execute(stmt.as_str(), &params)?;
        }
        Ok(())
    }

    /// Ensures each path has exactly the specified number of records.
    pub fn manage_stream_table(
        &mut self,
        specified_paths: &[String],
        specified_lengths: &[i32],
    ) -> Result<(), Error> {
        for (path, &target) in specified_paths.iter().zip(specified_lengths) {
            // Count existing
            let count_stmt = format!("SELECT COUNT(*) FROM \"{}\" WHERE path = $1;", self.table_name);
            let row = self.client.query_one(&count_stmt, &[&path])?;
            let current: i64 = row.get(0);
            let diff = target as i64 - current;

            if diff < 0 {
                // delete oldest |diff|
                let del = format!(r#"
                    DELETE FROM "{}"
                    WHERE path = $1 AND recorded_at IN (
                        SELECT recorded_at FROM "{}"
                        WHERE path = $1 ORDER BY recorded_at ASC LIMIT $2
                    );
                "#, self.table_name, self.table_name);
                self.client.execute(&del, &[&path, &(-diff as i64)])?;
            } else if diff > 0 {
                // insert diff new null entries
                let ins = format!(r#"
                    INSERT INTO "{}" (path, recorded_at, data, valid)
                    VALUES ($1, NOW(), $2, FALSE);
                "#, self.table_name);
                for _ in 0..diff {
                    // JSONB null => Value::Null
                    self.client.execute(&ins, &[&path, &Json(&Value::Null)])?;
                }
            }
        }
        Ok(())
    }

    /// Synchronize stream table with KB definitions.
    pub fn check_installation(&mut self) -> Result<(), Error> {
        // 1) fetch distinct stream paths
        let p = format!("SELECT DISTINCT path::text FROM \"{}\";", self.table_name);
        let rows: Vec<Row> = self.client.query(&p, &[])?;
        let unique_paths: Vec<String> = rows.iter().map(|r| r.get(0)).collect();

        // 2) fetch KB-defined fields
        let kq = format!(
            "SELECT path, properties->>'stream_length' as sl FROM \"{}\" WHERE label='KB_STREAM_FIELD';",
            self.database
        );
        let krows: Vec<Row> = self.client.query(&kq, &[])?;
        let mut specified_paths = Vec::new();
        let mut specified_lengths = Vec::new();
        for r in krows {
            let p: String = r.get(0);
            let sl: i32 = r.get::<_, String>(1).parse().unwrap_or(0);
            specified_paths.push(p);
            specified_lengths.push(sl);
        }

        // 3) diff
        let invalid: Vec<String> = unique_paths.iter().filter(|p| !specified_paths.contains(p)).cloned().collect();

        self.remove_invalid_stream_fields(&invalid, 500)?;
        self.manage_stream_table(&specified_paths, &specified_lengths)?;
        Ok(())
    }
}
