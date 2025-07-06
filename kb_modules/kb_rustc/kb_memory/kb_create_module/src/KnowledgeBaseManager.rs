```rust
// src/lib.rs

use tokio_postgres::{Client, NoTls, Error, Row};
use tokio_postgres::types::Json;
use tokio::spawn;
use serde_json::Value;

/// Manages a Postgres‐ltree KB schema and basic CRUD for KBs, nodes, links, and mounts.
pub struct KnowledgeBaseManager {
    client: Client,
    table_name: String,
}

impl KnowledgeBaseManager {
    /// Connects, enables ltree extension, and (re)creates all tables if needed.
    pub async fn new(table_name: &str, conn_str: &str) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await?;
        // Spawn connection task
        spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Ensure ltree extension exists
        client.batch_execute("CREATE EXTENSION IF NOT EXISTS ltree;").await?;

        Ok(Self {
            client,
            table_name: table_name.to_string(),
        })
    }

    /// Expose a mutable reference to the underlying client.
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Expose the configured table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Execute arbitrary SQL batch commands
    pub async fn batch_execute(&mut self, sql: &str) -> Result<(), Error> {
        self.client.batch_execute(sql).await
    }

    /// Drop a specific table in public schema
    pub async fn drop_table(&mut self, name: &str) -> Result<(), Error> {
        let stmt = format!("DROP TABLE IF EXISTS public.\"{}\" CASCADE;", name);
        self.client.batch_execute(&stmt).await
    }

    /// Insert into `<table_name>_info`
    pub async fn add_kb(&mut self, kb_name: &str, description: Option<&str>) -> Result<u64, Error> {
        let info_table = format!("{}_info", self.table_name);
        let desc = description.unwrap_or("");
        let stmt = format!(
            "INSERT INTO \"{}\" (knowledge_base, description) VALUES ($1, $2) ON CONFLICT (knowledge_base) DO NOTHING;",
            info_table
        );
        self.client.execute(&stmt, &[&kb_name, &desc]).await
    }

    /// Insert a node into `<table_name>`
    pub async fn add_node(
        &mut self,
        kb_name: &str,
        label: &str,
        name: &str,
        properties: Option<&Value>,
        data: Option<&Value>,
        path: &str,
    ) -> Result<u64, Error> {
        // Validate KB exists
        let info_table = format!("{}_info", self.table_name);
        let check = format!(
            "SELECT 1 FROM \"{}\" WHERE knowledge_base = $1;",
            info_table
        );
        if self.client.query_opt(&check, &[&kb_name]).await?.is_none() {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("KB '{}' not found", kb_name),
            )));
        }

        let stmt = format!(
            "INSERT INTO \"{}\" \
             (knowledge_base, label, name, properties, data, has_link, path) \
             VALUES ($1, $2, $3, $4, $5, FALSE, $6);",
            self.table_name
        );
        let props = properties.map(|v| Json(v.clone()));
        let data = data.map(|v| Json(v.clone()));
        self.client
            .execute(&stmt, &[&kb_name, &label, &name, &props, &data, &path])
            .await
    }

    /// Add a link in `<table_name>_link` and flip the `has_link` flag.
    pub async fn add_link(
        &mut self,
        parent_kb: &str,
        parent_path: &str,
        link_name: &str,
    ) -> Result<u64, Error> {
        let link_tbl = format!("{}_link", self.table_name);
        let stmt = format!(
            "INSERT INTO \"{}\" (parent_node_kb, parent_path, link_name) VALUES ($1, $2, $3);",
            link_tbl
        );
        self.client.execute(&stmt, &[&parent_kb, &parent_path, &link_name]).await?;

        let upd = format!(
            "UPDATE \"{}\" SET has_link = TRUE WHERE path = $1;",
            self.table_name
        );
        self.client.execute(&upd, &[&parent_path]).await
    }

    /// Add a link‐mount under the current header path.
    pub async fn add_link_mount(
        &mut self,
        kb: &str,
        path: &str,
        link_mount_name: &str,
        description: &str,
    ) -> Result<u64, Error> {
        let mount_tbl = format!("{}_link_mount", self.table_name);
        let stmt = format!(
            "INSERT INTO \"{}\" (link_name, knowledge_base, mount_path, description) VALUES ($1, $2, $3, $4);",
            mount_tbl
        );
        self.client.execute(&stmt, &[&link_mount_name, &kb, &path, &description]).await?;

        let upd = format!(
            "UPDATE \"{}\" SET has_link_mount = TRUE WHERE knowledge_base = $1 AND path = $2;",
            self.table_name
        );
        self.client.execute(&upd, &[&kb, &path]).await
    }

    /// Close the connection (dropping finalizes it).
    pub async fn disconnect(self) {
        // Client drops here
    }
}
```
