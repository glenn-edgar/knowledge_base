use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::{Client, Row};
use tokio_postgres::types::ToSql;

// Assuming KBSearch is defined with client: Option<Arc<Client>>
use crate::KBSearch; // Adjust import as necessary

#[derive(Clone)]
pub struct StreamRecord {
    pub id: i64,
    pub path: String,
    pub recorded_at: DateTime<Utc>,
    pub data: Value,
    pub valid: bool,
}

pub struct KBStream {
    kb_search: KBSearch,
    client: Arc<Client>,
    base_table: String,
}

impl KBStream {
    pub fn new(mut kb_search: KBSearch, database: String) -> Result<Self> {
        let client = kb_search.client.clone().ok_or(anyhow!("Not connected"))?;
        Ok(Self {
            kb_search,
            client,
            base_table: format!("{}_stream", database),
        })
    }

    async fn execute_query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        Ok(self.client.query(query, params).await?)
    }

    async fn execute_single(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        let rows = self.client.query(query, params).await?;
        Ok(rows.into_iter().next())
    }

    async fn sleep(&self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }

    pub async fn find_stream_id(
        &mut self,
        kb: Option<String>,
        node_name: Option<String>,
        properties: Option<HashMap<String, Value>>,
        node_path: Option<String>,
    ) -> Result<Row> {
        let results = self
            .find_stream_ids(kb, node_name, properties, node_path)
            .await?;
        if results.is_empty() {
            return Err(anyhow!(
                "No stream node found matching parameters: name={}, properties={}, path={}",
                node_name.unwrap_or_default(),
                serde_json::to_string(&properties).unwrap_or_default(),
                node_path.unwrap_or_default()
            ));
        }
        if results.len() > 1 {
            return Err(anyhow!(
                "Multiple stream nodes ({}) found matching parameters: name={}, properties={}, path={}",
                results.len(),
                node_name.unwrap_or_default(),
                serde_json::to_string(&properties).unwrap_or_default(),
                node_path.unwrap_or_default()
            ));
        }
        Ok(results[0].clone())
    }

    pub async fn find_stream_ids(
        &mut self,
        kb: Option<String>,
        node_name: Option<String>,
        properties: Option<HashMap<String, Value>>,
        node_path: Option<String>,
    ) -> Result<Vec<Row>> {
        self.kb_search.clear_filters();
        self.kb_search.search_label("KB_STREAM_FIELD".to_string());
        if let Some(k) = kb {
            self.kb_search.search_kb(k);
        }
        if let Some(n) = node_name {
            self.kb_search.search_name(n);
        }
        if let Some(props) = properties {
            for (k, v) in props {
                self.kb_search.search_property_value(k, v);
            }
        }
        if let Some(p) = node_path {
            self.kb_search.search_path(p);
        }

        let node_ids = self.kb_search.execute_query().await?;
        if node_ids.is_empty() {
            return Err(anyhow!(
                "No stream nodes found matching parameters: name={}, properties={}, path={}",
                node_name.unwrap_or_default(),
                serde_json::to_string(&properties).unwrap_or_default(),
                node_path.unwrap_or_default()
            ));
        }
        Ok(node_ids)
    }

    pub fn find_stream_table_keys(&self, rows: &[Row]) -> Vec<String> {
        if rows.is_empty() {
            return vec![];
        }
        rows.iter()
            .filter_map(|r| r.try_get("path").ok())
            .collect()
    }

    pub async fn push_stream_data(
        &self,
        path: String,
        data: Value,
        max_retries: u32,
        retry_delay: u64,
    ) -> Result<HashMap<String, Value>> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        if !data.is_object() {
            return Err(anyhow!("Data must be an object"));
        }

        for attempt in 1..=max_retries {
            let tx = self.client.transaction().await?;

            let count_row = tx
                .query_one(
                    &format!("SELECT COUNT(*) AS count FROM {} WHERE path = $1", self.base_table),
                    &[&path],
                )
                .await?;
            let total: i64 = count_row.get("count");
            if total == 0 {
                tx.rollback().await?;
                return Err(anyhow!(
                    "No records found for path='{}'. Must pre-allocate.",
                    path
                ));
            }

            let row_opt = tx
                .query_opt(
                    &format!(
                        "
                        SELECT id, recorded_at, valid
                        FROM {}
                        WHERE path = $1
                        ORDER BY recorded_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    ",
                        self.base_table
                    ),
                    &[&path],
                )
                .await?;

            let row = match row_opt {
                Some(r) => r,
                None => {
                    tx.rollback().await?;
                    if attempt < max_retries {
                        self.sleep(retry_delay).await;
                        continue;
                    } else {
                        return Err(anyhow!(
                            "Could not lock any row for path='{}' after {} attempts",
                            path,
                            max_retries
                        ));
                    }
                }
            };

            let id: i64 = row.get("id");
            let prev_recorded_at: DateTime<Utc> = row.get("recorded_at");
            let was_valid: bool = row.get("valid");

            let upd_opt = tx
                .query_opt(
                    &format!(
                        "
                        UPDATE {}
                        SET data = $1, recorded_at = NOW(), valid = TRUE
                        WHERE id = $2
                        RETURNING id, path, recorded_at, data, valid
                    ",
                        self.base_table
                    ),
                    &[&data, &id],
                )
                .await?;

            let upd = match upd_opt {
                Some(u) => u,
                None => {
                    tx.rollback().await?;
                    return Err(anyhow!("Failed to update record id={}", id));
                }
            };

            tx.commit().await?;

            let mut result = HashMap::new();
            result.insert("id".to_string(), json!(upd.get::<_, i64>("id")));
            result.insert("path".to_string(), json!(upd.get::<_, String>("path")));
            result.insert("recorded_at".to_string(), json!(upd.get::<_, DateTime<Utc>>("recorded_at").to_rfc3339()));
            result.insert("data".to_string(), upd.get::<_, Value>("data"));
            result.insert("valid".to_string(), json!(upd.get::<_, bool>("valid")));
            result.insert("previous_recorded_at".to_string(), json!(prev_recorded_at.to_rfc3339()));
            result.insert("was_previously_valid".to_string(), json!(was_valid));
            result.insert("operation".to_string(), json!("circular_buffer_replace"));

            return Ok(result);
        }
        Err(anyhow!("Unexpected error in push_stream_data"))
    }

    pub async fn get_latest_stream_data(&self, path: String) -> Result<Option<StreamRecord>> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        let row_opt = self
            .execute_single(
                &format!(
                    "
                    SELECT id, path, recorded_at, data, valid
                    FROM {}
                    WHERE path = $1 AND valid = TRUE
                    ORDER BY recorded_at DESC
                    LIMIT 1
                ",
                    self.base_table
                ),
                &[&path],
            )
            .await?;
        Ok(row_opt.map(|row| StreamRecord {
            id: row.get("id"),
            path: row.get("path"),
            recorded_at: row.get("recorded_at"),
            data: row.get("data"),
            valid: row.get("valid"),
        }))
    }

    pub async fn get_stream_data_count(&self, path: String, include_invalid: bool) -> Result<i64> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        let query = if include_invalid {
            format!("SELECT COUNT(*) AS count FROM {} WHERE path = $1", self.base_table)
        } else {
            format!(
                "SELECT COUNT(*) AS count FROM {} WHERE path = $1 AND valid = TRUE",
                self.base_table
            )
        };
        let row = self.execute_single(&query, &[&path]).await?.ok_or(anyhow!("No row"))?;
        Ok(row.get("count"))
    }

    pub async fn clear_stream_data(
        &self,
        path: String,
        older_than: Option<DateTime<Utc>>,
    ) -> Result<HashMap<String, Value>> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        let (query, params): (String, Vec<&(dyn ToSql + Sync)>) = match older_than {
            Some(dt) => (
                format!(
                    "
                    UPDATE {}
                    SET valid = FALSE
                    WHERE path = $1 AND recorded_at < $2 AND valid = TRUE
                    RETURNING id, recorded_at
                ",
                    self.base_table
                ),
                vec![&path, &dt],
            ),
            None => (
                format!(
                    "
                    UPDATE {}
                    SET valid = FALSE
                    WHERE path = $1 AND valid = TRUE
                    RETURNING id, recorded_at
                ",
                    self.base_table
                ),
                vec![&path],
            ),
        };
        let recs = self.execute_query(&query, &params).await?;

        let mut result = HashMap::new();
        result.insert("success".to_string(), json!(true));
        result.insert("clearedCount".to_string(), json!(recs.len()));
        result.insert(
            "clearedRecords".to_string(),
            json!(recs
                .iter()
                .map(|r| {
                    let mut m = HashMap::new();
                    m.insert("id".to_string(), json!(r.get::<_, i64>("id")));
                    m.insert("recorded_at".to_string(), json!(r.get::<_, DateTime<Utc>>("recorded_at").to_rfc3339()));
                    m
                })
                .collect::<Vec<_>>()),
        );

        Ok(result)
    }

    pub async fn list_stream_data(
        &self,
        path: String,
        limit: Option<usize>,
        offset: usize,
        recorded_after: Option<DateTime<Utc>>,
        recorded_before: Option<DateTime<Utc>>,
        order: String,
    ) -> Result<Vec<StreamRecord>> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        if order != "ASC" && order != "DESC" {
            return Err(anyhow!("Order must be 'ASC' or 'DESC'"));
        }

        let mut query = format!(
            "
            SELECT id, path, recorded_at, data, valid
            FROM {}
            WHERE path = $1 AND valid = TRUE
        ",
            self.base_table
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&path];
        if let Some(after) = recorded_after {
            params.push(&after);
            query += &format!(" AND recorded_at >= ${}", params.len());
        }
        if let Some(before) = recorded_before {
            params.push(&before);
            query += &format!(" AND recorded_at <= ${}", params.len());
        }
        query += &format!(" ORDER BY recorded_at {}", order);
        if let Some(l) = limit {
            if l > 0 {
                params.push(&(l as i32));
                query += &format!(" LIMIT ${}", params.len());
            }
        }
        if offset > 0 {
            params.push(&(offset as i32));
            query += &format!(" OFFSET ${}", params.len());
        }

        let rows = self.execute_query(&query, &params).await?;
        Ok(rows
            .into_iter()
            .map(|row| StreamRecord {
                id: row.get("id"),
                path: row.get("path"),
                recorded_at: row.get("recorded_at"),
                data: row.get("data"),
                valid: row.get("valid"),
            })
            .collect())
    }

    pub async fn get_stream_data_range(
        &self,
        path: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<StreamRecord>> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        if start_time >= end_time {
            return Err(anyhow!("start_time must be before end_time"));
        }

        let query = format!(
            "
            SELECT id, path, recorded_at, data, valid
            FROM {}
            WHERE path = $1
              AND recorded_at >= $2
              AND recorded_at <= $3
              AND valid = TRUE
            ORDER BY recorded_at ASC
        ",
            self.base_table
        );
        let rows = self
            .execute_query(&query, &[&path, &start_time, &end_time])
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| StreamRecord {
                id: row.get("id"),
                path: row.get("path"),
                recorded_at: row.get("recorded_at"),
                data: row.get("data"),
                valid: row.get("valid"),
            })
            .collect())
    }

    pub async fn get_stream_statistics(
        &self,
        path: String,
        include_invalid: bool,
    ) -> Result<HashMap<String, Value>> {
        if path.is_empty() {
            return Err(anyhow!("Path cannot be empty"));
        }
        let stats_query = if include_invalid {
            format!(
                "
                SELECT 
                  COUNT(*) AS total_records,
                  COUNT(CASE WHEN valid THEN 1 END) AS valid_records,
                  COUNT(CASE WHEN NOT valid THEN 1 END) AS invalid_records,
                  MIN(CASE WHEN valid THEN recorded_at END) AS earliest_valid_recorded,
                  MAX(CASE WHEN valid THEN recorded_at END) AS latest_valid_recorded,
                  MIN(recorded_at) AS earliest_recorded_overall,
                  MAX(recorded_at) AS latest_recorded_overall,
                  AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) AS avg_interval_seconds_all,
                  AVG(CASE WHEN valid THEN EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at))) END) AS avg_interval_seconds_valid
                FROM {}
                WHERE path = $1
            ",
                self.base_table
            )
        } else {
            format!(
                "
                SELECT 
                  COUNT(*) AS valid_records,
                  MIN(recorded_at) AS earliest_recorded,
                  MAX(recorded_at) AS latest_recorded,
                  AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) AS avg_interval_seconds
                FROM {}
                WHERE path = $1 AND valid = TRUE
            ",
                self.base_table
            )
        };
        let row_opt = self.execute_single(&stats_query, &[&path]).await?;
        let mut stats = HashMap::new();
        if let Some(row) = row_opt {
            for col in row.columns() {
                let name = col.name().to_string();
                let val: Value = match row.try_get(&name) {
                    Ok(v @ Some(_)) => match col.type_() {
                        &tokio_postgres::types::Type::INT8 => json!(v.unwrap() as i64),
                        &tokio_postgres::types::Type::FLOAT8 => json!(v.unwrap() as f64),
                        &tokio_postgres::types::Type::TIMESTAMPTZ => json!(v.unwrap::<DateTime<Utc>>().to_rfc3339()),
                        _ => json!(null),
                    },
                    _ => json!(null),
                };
                stats.insert(name, val);
            }
        }
        Ok(stats)
    }

    pub async fn get_stream_data_by_id(&self, record_id: i64) -> Result<Option<HashMap<String, Value>>> {
        let query = format!(
            "
            SELECT id, path, recorded_at, data
            FROM {}
            WHERE id = $1
        ",
            self.base_table
        );
        let row_opt = self.execute_single(&query, &[&record_id]).await?;
        Ok(row_opt.map(|row| {
            let mut map = HashMap::new();
            map.insert("id".to_string(), json!(row.get::<_, i64>("id")));
            map.insert("path".to_string(), json!(row.get::<_, String>("path")));
            map.insert("recorded_at".to_string(), json!(row.get::<_, DateTime<Utc>>("recorded_at").to_rfc3339()));
            map.insert("data".to_string(), row.get::<_, Value>("data"));
            map
        }))
    }
}

