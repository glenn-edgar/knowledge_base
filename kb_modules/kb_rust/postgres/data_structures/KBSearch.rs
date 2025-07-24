use serde_json::{json, Value};
use std::collections::HashMap;
use tokio_postgres::{Client, NoTls, Row};
use tokio_postgres::types::ToSql;

struct Filter {
    condition: String,
    params: Vec<Box<dyn ToSql + Sync + Send>>,
}

pub struct KBSearch {
    pub path: Vec<String>,
    pub host: String,
    pub port: u16,
    pub db_name: String,
    pub user: String,
    pub password: String,
    pub base_table: String,
    pub link_table: String,
    pub link_mount_table: String,
    pub filters: Vec<Filter>,
    pub results: Option<Vec<Row>>,
    pub path_values: HashMap<String, Value>,
    pub client: Option<Client>,
}

impl KBSearch {
    pub async fn new(
        host: String,
        port: u16,
        db_name: String,
        user: String,
        password: String,
        base_table: String,
    ) -> Result<Self, tokio_postgres::Error> {
        let mut this = Self {
            path: Vec::new(),
            host,
            port,
            db_name,
            user,
            password,
            base_table: base_table.clone(),
            link_table: format!("{}_link", base_table),
            link_mount_table: format!("{}_link_mount", base_table),
            filters: Vec::new(),
            results: None,
            path_values: HashMap::new(),
            client: None,
        };
        this._connect().await?;
        Ok(this)
    }

    async fn _connect(&mut self) -> Result<(), tokio_postgres::Error> {
        let connect_string = format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.db_name, self.user, self.password
        );
        let (client, connection) = tokio_postgres::connect(&connect_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("Error in database connection: {}", err);
            }
        });
        self.client = Some(client);
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<(), tokio_postgres::Error> {
        if let Some(client) = self.client.take() {
            client.close().await?;
        }
        Ok(())
    }

    pub fn get_conn(&self) -> &Client {
        self.client
            .as_ref()
            .expect("Not connected to database. Call _connect() first.")
    }

    pub fn clear_filters(&mut self) {
        self.filters = Vec::new();
        self.results = None;
    }

    pub fn search_kb(&mut self, kb: String) {
        self.filters.push(Filter {
            condition: "knowledge_base = #".to_string(),
            params: vec![Box::new(kb)],
        });
    }

    pub fn search_label(&mut self, label: String) {
        self.filters.push(Filter {
            condition: "label = #".to_string(),
            params: vec![Box::new(label)],
        });
    }

    pub fn search_name(&mut self, name: String) {
        self.filters.push(Filter {
            condition: "name = #".to_string(),
            params: vec![Box::new(name)],
        });
    }

    pub fn search_property_key(&mut self, key: String) {
        self.filters.push(Filter {
            condition: "properties::jsonb ? #".to_string(),
            params: vec![Box::new(key)],
        });
    }

    pub fn search_property_value(&mut self, key: String, value: Value) {
        let json_object = json!({ key: value });
        self.filters.push(Filter {
            condition: "properties::jsonb @> #::jsonb".to_string(),
            params: vec![Box::new(json_object.to_string())],
        });
    }

    pub fn search_starting_path(&mut self, starting_path: String) {
        self.filters.push(Filter {
            condition: "path <@ #".to_string(),
            params: vec![Box::new(starting_path)],
        });
    }

    pub fn search_path(&mut self, path_expr: String) {
        self.filters.push(Filter {
            condition: "path ~ #".to_string(),
            params: vec![Box::new(path_expr)],
        });
    }

    pub fn search_has_link(&mut self) {
        self.filters.push(Filter {
            condition: "has_link = TRUE".to_string(),
            params: vec![],
        });
    }

    pub fn search_has_link_mount(&mut self) {
        self.filters.push(Filter {
            condition: "has_link_mount = TRUE".to_string(),
            params: vec![],
        });
    }

    pub async fn execute_query(&mut self) -> Result<Vec<Row>, tokio_postgres::Error> {
        let client = self.get_conn();
        let column_str = "*";
        if self.filters.is_empty() {
            let res = client
                .query(
                    &format!("SELECT {} FROM {}", column_str, self.base_table),
                    &[],
                )
                .await?;
            self.results = Some(res.clone());
            return Ok(res);
        }
        let mut cte_parts = vec![format!(
            "base_data AS (SELECT {} FROM {})",
            column_str, self.base_table
        )];
        let mut combined_params: Vec<&(dyn ToSql + Sync)> = vec![];
        let mut param_count: usize = 0;
        for (i, filt) in self.filters.iter().enumerate() {
            let mut cond = filt.condition.clone();
            for _ in 0..filt.params.len() {
                param_count += 1;
                let placeholder = format!("${}", param_count);
                cond = cond.replacen("#", &placeholder, 1);
            }
            let cte_name = format!("filter_{}", i);
            let prev = if i == 0 {
                "base_data".to_string()
            } else {
                format!("filter_{}", i - 1)
            };
            cte_parts.push(format!(
                "{} AS (SELECT {} FROM {} WHERE {})",
                cte_name, column_str, prev, cond
            ));
            for param in &filt.params {
                combined_params.push(&**param);
            }
        }
        let final_query = format!(
            "WITH {}\nSELECT {} FROM filter_{}",
            cte_parts.join(",\n"),
            column_str,
            self.filters.len() - 1
        );
        let res = client.query(&final_query, &combined_params).await?;
        self.results = Some(res.clone());
        Ok(res)
    }

    pub fn find_path_values(&self, key_data: &[Row]) -> Vec<String> {
        key_data.iter().map(|r| r.get("path")).collect()
    }

    pub fn get_results(&self) -> Vec<Row> {
        self.results.clone().unwrap_or_default()
    }

    pub fn find_description(&self, key_data: &[Row]) -> Vec<HashMap<String, String>> {
        key_data
            .iter()
            .map(|r| {
                let props: Value = r.get("properties");
                let description = props["description"].as_str().unwrap_or("").to_string();
                let mut map = HashMap::new();
                map.insert(r.get("path"), description);
                map
            })
            .collect()
    }

    pub async fn find_description_paths(
        &self,
        path_array: Vec<String>,
    ) -> Result<HashMap<String, Value>, tokio_postgres::Error> {
        let client = self.get_conn();
        let paths = path_array;
        if paths.is_empty() {
            return Ok(HashMap::new());
        }
        let (query, params_vec): (String, Vec<&(dyn ToSql + Sync)>) = if paths.len() == 1 {
            (
                format!("SELECT path, data FROM {} WHERE path = $1", self.base_table),
                vec![&paths[0]],
            )
        } else {
            let placeholders: String = (1..=paths.len())
                .map(|i| format!("${}", i))
                .collect::<Vec<_>>()
                .join(", ");
            let params: Vec<&(dyn ToSql + Sync)> = paths
                .iter()
                .map(|p| p as &(dyn ToSql + Sync))
                .collect();
            (
                format!(
                    "SELECT path, data FROM {} WHERE path IN ({})",
                    self.base_table, placeholders
                ),
                params,
            )
        };
        let res = client.query(&query, &params_vec).await?;
        let mut output: HashMap<String, Value> = HashMap::new();
        for row in res {
            output.insert(row.get("path"), row.get("data"));
        }
        for p in &paths {
            if !output.contains_key(p) {
                output.insert(p.clone(), Value::Null);
            }
        }
        Ok(output)
    }

    pub fn decode_link_nodes(&self, path: &str) -> (String, Vec<(String, String)>) {
        if path.is_empty() {
            panic!("Path must be a non-empty string");
        }
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() < 3 {
            panic!(
                "Path must have at least 3 elements (kb.link.name), got {}",
                parts.len()
            );
        }
        let rem = parts.len() - 1;
        if rem % 2 != 0 {
            panic!(
                "After kb identifier, must have even number of elements (link/name pairs), got {}",
                rem
            );
        }
        let kb = parts[0].to_string();
        let mut pairs: Vec<(String, String)> = Vec::new();
        let mut i = 1;
        while i < parts.len() {
            pairs.push((parts[i].to_string(), parts[i + 1].to_string()));
            i += 2;
        }
        (kb, pairs)
    }
}

