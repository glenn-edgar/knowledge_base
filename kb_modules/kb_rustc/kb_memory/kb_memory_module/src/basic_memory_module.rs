use std::collections::HashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeNode {
    pub path: String,
    pub data: Value,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub path: String,
    pub data: Value,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeStats {
    pub total_nodes: usize,
    pub max_depth: usize,
    pub avg_depth: f64,
    pub root_nodes: usize,
    pub leaf_nodes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStats {
    pub imported: usize,
    pub exported: usize,
}

#[derive(Debug)]
pub enum KbError {
    InvalidPath(String),
    DatabaseError(String),
    KnowledgeBaseExists(String),
    PathNotFound(String),
    ValidationError(String),
}

impl fmt::Display for KbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KbError::InvalidPath(path) => write!(f, "Invalid ltree path: {}", path),
            KbError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            KbError::KnowledgeBaseExists(name) => write!(f, "Knowledge base {} already exists", name),
            KbError::PathNotFound(path) => write!(f, "Path {} does not exist", path),
            KbError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl Error for KbError {}

pub struct BasicConstructDB {
    data: HashMap<String, TreeNode>,
    kb_dict: HashMap<String, HashMap<String, Value>>,
    host: String,
    port: u16,
    dbname: String,
    user: String,
    password: String,
    table_name: String,
    //connection_params: HashMap<String, Value>,
}

impl BasicConstructDB {
    pub fn new(
        host: String,
        port: u16,
        dbname: String,
        user: String,
        password: String,
        table_name: String,
    ) -> Self {
        //let mut connection_params = HashMap::new();
        //connection_params.insert("host".to_string(), Value::String(host.clone()));
        //connection_params.insert("port".to_string(), Value::Number(port.into()));
        //connection_params.insert("dbname".to_string(), Value::String(dbname.clone()));
        //connection_params.insert("user".to_string(), Value::String(user.clone()));
        //connection_params.insert("password".to_string(), Value::String(password.clone()));

        Self {
            data: HashMap::new(),
            kb_dict: HashMap::new(),
            host,
            port,
            dbname,
            user,
            password,
            table_name,
            //connection_params,
        }
    }

    pub fn add_kb(&mut self, kb_name: &str, description: &str) -> Result<(), KbError> {
        if self.kb_dict.contains_key(kb_name) {
            return Err(KbError::KnowledgeBaseExists(kb_name.to_string()));
        }
        
        let mut kb_info = HashMap::new();
        kb_info.insert("description".to_string(), Value::String(description.to_string()));
        self.kb_dict.insert(kb_name.to_string(), kb_info);
        Ok(())
    }

    pub fn validate_path(&self, path: &str) -> bool {
        if path.is_empty() {
            return false;
        }

        // ltree labels must start with letter or underscore, then alphanumeric and underscores
        let pattern = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$").unwrap();
        if !pattern.is_match(path) {
            return false;
        }

        // Check each label length
        let labels: Vec<&str> = path.split('.').collect();
        for label in labels {
            if label.len() < 1 || label.len() > 256 {
                return false;
            }
        }
        true
    }

    pub fn path_depth(&self, path: &str) -> usize {
        path.split('.').count()
    }

    pub fn path_labels(&self, path: &str) -> Vec<String> {
        path.split('.').map(|s| s.to_string()).collect()
    }

    pub fn subpath(&self, path: &str, start: i32, length: Option<i32>) -> String {
        let labels = self.path_labels(path);
        let start_idx = if start < 0 {
            (labels.len() as i32 + start) as usize
        } else {
            start as usize
        };

        match length {
            None => labels[start_idx..].join("."),
            Some(len) => {
                let end_idx = std::cmp::min(start_idx + len as usize, labels.len());
                labels[start_idx..end_idx].join(".")
            }
        }
    }

    pub fn convert_ltree_query_to_regex(&self, query: &str) -> String {
        // Handle ltxtquery format (word1@word2@word3)
        if query.contains('@') && !query.starts_with('@') && !query.ends_with('@') {
            return self.convert_simple_pattern(&query.replace('@', "."));
        }
        self.convert_lquery_pattern(query)
    }

    fn convert_lquery_pattern(&self, pattern: &str) -> String {
        // Escape special regex characters first
        let mut result = regex::escape(pattern);

        // Convert ltree-specific patterns
        // *{n,m} - between n and m levels
        let re1 = Regex::new(r"\\\*\\\{(\d+),(\d+)\\\}").unwrap();
        result = re1.replace_all(&result, |caps: &regex::Captures| {
            format!("([^.]+\\.){{{},{}}}", &caps[1], &caps[2])
        }).to_string();

        // *{n,} - n or more levels
        let re2 = Regex::new(r"\\\*\\\{(\d+),\\\}").unwrap();
        result = re2.replace_all(&result, |caps: &regex::Captures| {
            format!("([^.]+\\.){{{},}}", &caps[1])
        }).to_string();

        // *{,m} - up to m levels
        let re3 = Regex::new(r"\\\*\\\{,(\d+)\\\}").unwrap();
        result = re3.replace_all(&result, |caps: &regex::Captures| {
            format!("([^.]+\\.){{0,{}}}", &caps[1])
        }).to_string();

        // *{n} - exactly n levels
        let re4 = Regex::new(r"\\\*\\\{(\d+)\\\}").unwrap();
        result = re4.replace_all(&result, |caps: &regex::Captures| {
            format!("([^.]+\\.){{{}}}", &caps[1])
        }).to_string();

        // ** - any number of levels (including zero)
        result = result.replace("\\*\\*", ".*");

        // * - exactly one level
        result = result.replace("\\*", "[^.]+");

        // {a,b,c} - choice between alternatives
        let re5 = Regex::new(r"\\\{([^}]+)\\\}").unwrap();
        result = re5.replace_all(&result, |caps: &regex::Captures| {
            format!("({})", caps[1].replace(',', "|"))
        }).to_string();

        // Remove trailing dots from quantified patterns
        let re6 = Regex::new(r"\\\.\)\{([^}]+)\}").unwrap();
        result = re6.replace_all(&result, "){$1}[^.]*").to_string();

        format!("^{}$", result)
    }

    fn convert_simple_pattern(&self, pattern: &str) -> String {
        let parts: Vec<&str> = pattern.split(".*").collect();
        let escaped_parts: Vec<String> = parts.iter().map(|part| regex::escape(part)).collect();
        let mut result = escaped_parts.join(".*");

        result = result.replace("\\*\\*", ".*");
        result = result.replace("\\*", "[^.]+");

        let re = Regex::new(r"\\\{([^}]+)\\\}").unwrap();
        result = re.replace_all(&result, |caps: &regex::Captures| {
            format!("({})", caps[1].replace(',', "|"))
        }).to_string();

        format!("^{}$", result)
    }

    pub fn ltree_match(&self, path: &str, query: &str) -> bool {
        let regex_pattern = self.convert_ltree_query_to_regex(query);
        Regex::new(&regex_pattern).map(|re| re.is_match(path)).unwrap_or(false)
    }

    pub fn ltxtquery_match(&self, path: &str, ltxtquery: &str) -> bool {
        let mut path_words = HashMap::new();
        for word in path.split('.') {
            path_words.insert(word, true);
        }

        let query = ltxtquery.trim();

        // Handle simple cases first
        if !query.contains('&') && !query.contains('|') && !query.contains('!') {
            return path_words.contains_key(query.trim());
        }

        // This is a simplified implementation for basic boolean operations
        if query.contains('&') {
            let words: Vec<&str> = query.split('&').collect();
            for word in words {
                if !path_words.contains_key(word.trim()) {
                    return false;
                }
            }
            return true;
        }

        if query.contains('|') {
            let words: Vec<&str> = query.split('|').collect();
            for word in words {
                if path_words.contains_key(word.trim()) {
                    return true;
                }
            }
            return false;
        }

        false
    }

    pub fn ltree_ancestor(&self, ancestor: &str, descendant: &str) -> bool {
        if ancestor == descendant {
            return false;
        }
        descendant.starts_with(&format!("{}.", ancestor))
    }

    pub fn ltree_descendant(&self, descendant: &str, ancestor: &str) -> bool {
        self.ltree_ancestor(ancestor, descendant)
    }

    pub fn ltree_ancestor_or_equal(&self, ancestor: &str, descendant: &str) -> bool {
        ancestor == descendant || self.ltree_ancestor(ancestor, descendant)
    }

    pub fn ltree_descendant_or_equal(&self, descendant: &str, ancestor: &str) -> bool {
        descendant == ancestor || self.ltree_descendant(descendant, ancestor)
    }

    pub fn ltree_concatenate(&self, path1: &str, path2: &str) -> String {
        if path1.is_empty() {
            return path2.to_string();
        }
        if path2.is_empty() {
            return path1.to_string();
        }
        format!("{}.{}", path1, path2)
    }

    pub fn nlevel(&self, path: &str) -> usize {
        path.split('.').count()
    }

    pub fn subltree(&self, path: &str, start: usize, end: usize) -> String {
        let labels: Vec<&str> = path.split('.').collect();
        if start >= labels.len() {
            return String::new();
        }
        let end_idx = std::cmp::min(end, labels.len());
        labels[start..end_idx].join(".")
    }

    pub fn subpath_func(&self, path: &str, offset: i32, length: Option<i32>) -> String {
        self.subpath(path, offset, length)
    }

    pub fn index_func(&self, path: &str, subpath: &str, offset: usize) -> Option<usize> {
        let labels: Vec<&str> = path.split('.').collect();
        let sub_labels: Vec<&str> = subpath.split('.').collect();

        for i in offset..=(labels.len().saturating_sub(sub_labels.len())) {
            let mut match_found = true;
            for j in 0..sub_labels.len() {
                if labels[i + j] != sub_labels[j] {
                    match_found = false;
                    break;
                }
            }
            if match_found {
                return Some(i);
            }
        }
        None
    }

    pub fn text2ltree(&self, text: &str) -> Result<String, KbError> {
        if self.validate_path(text) {
            Ok(text.to_string())
        } else {
            Err(KbError::ValidationError(format!("Cannot convert '{}' to valid ltree format", text)))
        }
    }

    pub fn ltree2text(&self, ltree_path: &str) -> String {
        ltree_path.to_string()
    }

    pub fn lca(&self, paths: &[String]) -> Option<String> {
        if paths.is_empty() {
            return None;
        }

        if paths.len() == 1 {
            return Some(paths[0].clone());
        }

        // Split all paths into labels
        let all_labels: Vec<Vec<&str>> = paths.iter().map(|p| p.split('.').collect()).collect();
        let min_length = all_labels.iter().map(|labels| labels.len()).min().unwrap_or(0);

        // Find common prefix
        let mut common_labels = Vec::new();
        for i in 0..min_length {
            let current_label = all_labels[0][i];
            let mut match_found = true;
            for j in 1..all_labels.len() {
                if all_labels[j][i] != current_label {
                    match_found = false;
                    break;
                }
            }
            if match_found {
                common_labels.push(current_label);
            } else {
                break;
            }
        }

        if common_labels.is_empty() {
            None
        } else {
            Some(common_labels.join("."))
        }
    }

    pub fn store(&mut self, path: &str, data: Value, created_at: Option<String>, updated_at: Option<String>) -> Result<(), KbError> {
        if !self.validate_path(path) {
            return Err(KbError::InvalidPath(path.to_string()));
        }

        self.data.insert(path.to_string(), TreeNode {
            path: path.to_string(),
            data,
            created_at,
            updated_at,
        });
        Ok(())
    }

    pub fn get(&self, path: &str) -> Result<Option<Value>, KbError> {
        if !self.validate_path(path) {
            return Err(KbError::InvalidPath(path.to_string()));
        }

        Ok(self.data.get(path).map(|node| node.data.clone()))
    }

    pub fn get_node(&self, path: &str) -> Result<Option<TreeNode>, KbError> {
        if !self.validate_path(path) {
            return Err(KbError::InvalidPath(path.to_string()));
        }

        Ok(self.data.get(path).cloned())
    }

    pub fn query(&self, pattern: &str) -> Vec<QueryResult> {
        let mut results = Vec::new();

        for (path, node) in &self.data {
            if self.ltree_match(path, pattern) {
                results.push(QueryResult {
                    path: path.clone(),
                    data: node.data.clone(),
                    created_at: node.created_at.clone(),
                    updated_at: node.updated_at.clone(),
                });
            }
        }

        results.sort_by(|a, b| a.path.cmp(&b.path));
        results
    }

    pub fn query_ltxtquery(&self, ltxtquery: &str) -> Vec<QueryResult> {
        let mut results = Vec::new();

        for (path, node) in &self.data {
            if self.ltxtquery_match(path, ltxtquery) {
                results.push(QueryResult {
                    path: path.clone(),
                    data: node.data.clone(),
                    created_at: node.created_at.clone(),
                    updated_at: node.updated_at.clone(),
                });
            }
        }

        results.sort_by(|a, b| a.path.cmp(&b.path));
        results
    }

    pub fn query_by_operator(&self, operator: &str, path1: &str, _path2: &str) -> Vec<QueryResult> {
        let mut results = Vec::new();

        match operator {
            "@>" => {
                // ancestor-of
                for (path, node) in &self.data {
                    if self.ltree_ancestor(path1, path) {
                        results.push(QueryResult {
                            path: path.clone(),
                            data: node.data.clone(),
                            created_at: node.created_at.clone(),
                            updated_at: node.updated_at.clone(),
                        });
                    }
                }
            }
            "<@" => {
                // descendant-of
                for (path, node) in &self.data {
                    if self.ltree_descendant(path, path1) {
                        results.push(QueryResult {
                            path: path.clone(),
                            data: node.data.clone(),
                            created_at: node.created_at.clone(),
                            updated_at: node.updated_at.clone(),
                        });
                    }
                }
            }
            "~" => return self.query(path1),
            "@@" => return self.query_ltxtquery(path1),
            _ => {}
        }

        results.sort_by(|a, b| a.path.cmp(&b.path));
        results
    }

    pub fn query_ancestors(&self, path: &str) -> Result<Vec<QueryResult>, KbError> {
        if !self.validate_path(path) {
            return Err(KbError::InvalidPath(path.to_string()));
        }

        let mut results = Vec::new();
        for (stored_path, node) in &self.data {
            if self.ltree_ancestor(stored_path, path) {
                results.push(QueryResult {
                    path: stored_path.clone(),
                    data: node.data.clone(),
                    created_at: node.created_at.clone(),
                    updated_at: node.updated_at.clone(),
                });
            }
        }

        results.sort_by(|a, b| {
            let a_depth = a.path.split('.').count();
            let b_depth = b.path.split('.').count();
            a_depth.cmp(&b_depth)
        });

        Ok(results)
    }

    pub fn query_descendants(&self, path: &str) -> Result<Vec<QueryResult>, KbError> {
        if !self.validate_path(path) {
            return Err(KbError::InvalidPath(path.to_string()));
        }

        let mut results = Vec::new();
        for (stored_path, node) in &self.data {
            if self.ltree_descendant(stored_path, path) {
                results.push(QueryResult {
                    path: stored_path.clone(),
                    data: node.data.clone(),
                    created_at: node.created_at.clone(),
                    updated_at: node.updated_at.clone(),
                });
            }
        }

        results.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(results)
    }

    pub fn query_subtree(&self, path: &str) -> Result<Vec<QueryResult>, KbError> {
        let mut results = Vec::new();

        // Add the node itself if it exists
        if self.exists(path) {
            if let Some(node) = self.data.get(path) {
                results.push(QueryResult {
                    path: path.to_string(),
                    data: node.data.clone(),
                    created_at: node.created_at.clone(),
                    updated_at: node.updated_at.clone(),
                });
            }
        }

        // Add all descendants
        let descendants = self.query_descendants(path)?;
        results.extend(descendants);

        results.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(results)
    }

    pub fn exists(&self, path: &str) -> bool {
        self.data.contains_key(path) && self.validate_path(path)
    }

    pub fn delete(&mut self, path: &str) -> bool {
        self.data.remove(path).is_some()
    }

    pub fn add_subtree(&mut self, path: &str, subtree: &[QueryResult]) -> Result<(), KbError> {
        if !self.validate_path(path) {
            return Err(KbError::InvalidPath(path.to_string()));
        }
        if !self.exists(path) {
            return Err(KbError::PathNotFound(path.to_string()));
        }

        for node in subtree {
            let new_path = format!("{}.{}", path, node.path);
            self.store(&new_path, node.data.clone(), node.created_at.clone(), node.updated_at.clone())?;
        }
        Ok(())
    }

    pub fn delete_subtree(&mut self, path: &str) -> usize {
        let mut to_delete = Vec::new();

        if self.data.contains_key(path) {
            to_delete.push(path.to_string());
        }

        // Find all descendants
        for stored_path in self.data.keys() {
            if self.ltree_descendant(stored_path, path) {
                to_delete.push(stored_path.clone());
            }
        }

        // Delete them
        let count = to_delete.len();
        for delete_path in to_delete {
            self.data.remove(&delete_path);
        }

        count
    }

    fn get_database_url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }

    pub async fn import_from_postgres(
        &mut self,
        table_name: &str,
        path_column: &str,
        data_column: &str,
        created_at_column: &str,
        updated_at_column: &str,
    ) -> Result<usize, KbError> {
        let pool = PgPool::connect(&self.get_database_url())
            .await
            .map_err(|e| KbError::DatabaseError(e.to_string()))?;

        // Check if table exists
        let exists_query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)";
        let exists: bool = sqlx::query_scalar(exists_query)
            .bind(table_name)
            .fetch_one(&pool)
            .await
            .map_err(|e| KbError::DatabaseError(e.to_string()))?;

        if !exists {
            return Err(KbError::DatabaseError(format!("Table '{}' does not exist", table_name)));
        }

        // Import data
        let query = format!(
            "SELECT {}::text as path, {}, {}::text as created_at, {}::text as updated_at FROM {} ORDER BY {}",
            path_column, data_column, created_at_column, updated_at_column, table_name, path_column
        );

        let rows = sqlx::query(&query)
            .fetch_all(&pool)
            .await
            .map_err(|e| KbError::DatabaseError(e.to_string()))?;

        let mut imported_count = 0;
        for row in rows {
            let path: String = row.get("path");
            let data_bytes: Option<Vec<u8>> = row.try_get(data_column).ok();
            let created_at: Option<String> = row.try_get("created_at").ok();
            let updated_at: Option<String> = row.try_get("updated_at").ok();

            let data = if let Some(bytes) = data_bytes {
                serde_json::from_slice(&bytes).unwrap_or(Value::Null)
            } else {
                Value::Null
            };

            if self.store(&path, data, created_at, updated_at).is_ok() {
                imported_count += 1;
            }
        }

        pool.close().await;
        Ok(imported_count)
    }

    pub async fn export_to_postgres(
        &self,
        table_name: &str,
        create_table: bool,
        clear_existing: bool,
    ) -> Result<usize, KbError> {
        let pool = PgPool::connect(&self.get_database_url())
            .await
            .map_err(|e| KbError::DatabaseError(e.to_string()))?;

        // Enable ltree extension
        sqlx::query("CREATE EXTENSION IF NOT EXISTS ltree")
            .execute(&pool)
            .await
            .map_err(|e| KbError::DatabaseError(e.to_string()))?;

        if create_table {
            // Create table with ltree support
            let create_table_query = format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    path LTREE UNIQUE NOT NULL,
                    data JSONB,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )",
                table_name
            );
            sqlx::query(&create_table_query)
                .execute(&pool)
                .await
                .map_err(|e| KbError::DatabaseError(e.to_string()))?;

            // Create indexes
            let path_index = format!("CREATE INDEX IF NOT EXISTS {}_path_idx ON {} USING GIST (path)", table_name, table_name);
            sqlx::query(&path_index)
                .execute(&pool)
                .await
                .map_err(|e| KbError::DatabaseError(e.to_string()))?;

            let data_index = format!("CREATE INDEX IF NOT EXISTS {}_data_idx ON {} USING GIN (data)", table_name, table_name);
            sqlx::query(&data_index)
                .execute(&pool)
                .await
                .map_err(|e| KbError::DatabaseError(e.to_string()))?;
        }

        if clear_existing {
            let truncate_query = format!("TRUNCATE TABLE {}", table_name);
            sqlx::query(&truncate_query)
                .execute(&pool)
                .await
                .map_err(|e| KbError::DatabaseError(e.to_string()))?;
        }

        // Export data
        let mut exported_count = 0;
        let insert_query = format!(
            "INSERT INTO {} (path, data, created_at, updated_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at",
            table_name
        );

        for (path, node) in &self.data {
            let data_json = serde_json::to_value(&node.data).unwrap_or(Value::Null);
            
            let result = sqlx::query(&insert_query)
                .bind(path)
                .bind(&data_json)
                .bind(&node.created_at)
                .bind(&node.updated_at)
                .execute(&pool)
                .await;

            if result.is_ok() {
                exported_count += 1;
            }
        }

        pool.close().await;
        Ok(exported_count)
    }

    pub async fn sync_with_postgres(&mut self, direction: &str) -> SyncStats {
        let mut stats = SyncStats {
            imported: 0,
            exported: 0,
        };

        if direction == "import" || direction == "both" {
            if let Ok(imported) = self.import_from_postgres(
                &self.table_name.clone(),
                "path",
                "data",
                "created_at",
                "updated_at",
            ).await {
                stats.imported = imported;
            }
        }

        if direction == "export" || direction == "both" {
            if let Ok(exported) = self.export_to_postgres(&self.table_name.clone(), true, false).await {
                stats.exported = exported;
            }
        }

        stats
    }

    pub fn get_stats(&self) -> TreeStats {
        if self.data.is_empty() {
            return TreeStats {
                total_nodes: 0,
                max_depth: 0,
                avg_depth: 0.0,
                root_nodes: 0,
                leaf_nodes: 0,
            };
        }

        let mut depths = Vec::new();
        let mut root_nodes = 0;

        for path in self.data.keys() {
            let depth = self.nlevel(path);
            depths.push(depth);
            if depth == 1 {
                root_nodes += 1;
            }
        }

        // Calculate max depth
        let max_depth = depths.iter().max().copied().unwrap_or(0);
        let total_depth: usize = depths.iter().sum();

        // Count leaf nodes (nodes with no children)
        let mut leaf_nodes = 0;
        for path in self.data.keys() {
            let mut has_children = false;
            for other_path in self.data.keys() {
                if self.ltree_ancestor(path, other_path) {
                    has_children = true;
                    break;
                }
            }
            if !has_children {
                leaf_nodes += 1;
            }
        }

        let avg_depth = if depths.is_empty() {
            0.0
        } else {
            total_depth as f64 / depths.len() as f64
        };

        TreeStats {
            total_nodes: self.data.len(),
            max_depth,
            avg_depth,
            root_nodes,
            leaf_nodes,
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn get_all_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self.data.keys().cloned().collect();
        paths.sort();
        paths
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path() {
        let db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        assert!(db.validate_path("root"));
        assert!(db.validate_path("root.child"));
        assert!(db.validate_path("root.child.grandchild"));
        assert!(db.validate_path("_underscore"));
        assert!(db.validate_path("with_123"));
        
        assert!(!db.validate_path(""));
        assert!(!db.validate_path("123invalid"));
        assert!(!db.validate_path("invalid-dash"));
        assert!(!db.validate_path("invalid space"));
        assert!(!db.validate_path(".invalid"));
        assert!(!db.validate_path("invalid."));
    }

    #[test]
    fn test_ltree_operations() {
        let db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        // Test ancestor/descendant relationships
        assert!(db.ltree_ancestor("root", "root.child"));
        assert!(db.ltree_ancestor("root.child", "root.child.grandchild"));
        assert!(!db.ltree_ancestor("root", "root"));
        assert!(!db.ltree_ancestor("root.child", "root"));

        assert!(db.ltree_descendant("root.child", "root"));
        assert!(db.ltree_descendant("root.child.grandchild", "root.child"));

        // Test path operations
        assert_eq!(db.path_depth("root"), 1);
        assert_eq!(db.path_depth("root.child"), 2);
        assert_eq!(db.path_depth("root.child.grandchild"), 3);

        assert_eq!(db.ltree_concatenate("root", "child"), "root.child");
        assert_eq!(db.ltree_concatenate("", "child"), "child");
        assert_eq!(db.ltree_concatenate("root", ""), "root");
    }

    #[test]
    fn test_store_and_retrieve() {
        let mut db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        let data = serde_json::json!({"name": "test", "value": 42});
        
        assert!(db.store("root", data.clone(), None, None).is_ok());
        assert!(db.exists("root"));
        
        let retrieved = db.get("root").unwrap().unwrap();
        assert_eq!(retrieved, data);
        
        let node = db.get_node("root").unwrap().unwrap();
        assert_eq!(node.path, "root");
        assert_eq!(node.data, data);
    }

    #[test]
    fn test_query_operations() {
        let mut db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        // Add test data
        let _ = db.store("root", serde_json::json!({"type": "root"}), None, None);
        let _ = db.store("root.child1", serde_json::json!({"type": "child"}), None, None);
        let _ = db.store("root.child2", serde_json::json!({"type": "child"}), None, None);
        let _ = db.store("root.child1.grandchild", serde_json::json!({"type": "grandchild"}), None, None);

        // Test pattern matching
        let results = db.query("root.*");
        assert_eq!(results.len(), 2); // child1 and child2

        let results = db.query("root.**");
        assert_eq!(results.len(), 3); // child1, child2, and grandchild

        // Test ancestor/descendant queries
        let ancestors = db.query_ancestors("root.child1.grandchild").unwrap();
        assert_eq!(ancestors.len(), 2); // root and root.child1

        let descendants = db.query_descendants("root").unwrap();
        assert_eq!(descendants.len(), 3); // child1, child2, and grandchild

        let subtree = db.query_subtree("root.child1").unwrap();
        assert_eq!(subtree.len(), 2); // child1 and grandchild
    }

    #[test]
    fn test_delete_operations() {
        let mut db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        // Add test data
        let _ = db.store("root", serde_json::json!({"type": "root"}), None, None);
        let _ = db.store("root.child1", serde_json::json!({"type": "child"}), None, None);
        let _ = db.store("root.child2", serde_json::json!({"type": "child"}), None, None);
        let _ = db.store("root.child1.grandchild", serde_json::json!({"type": "grandchild"}), None, None);

        assert_eq!(db.size(), 4);

        // Delete single node
        assert!(db.delete("root.child2"));
        assert_eq!(db.size(), 3);
        assert!(!db.exists("root.child2"));

        // Delete subtree
        let deleted_count = db.delete_subtree("root.child1");
        assert_eq!(deleted_count, 2); // child1 and grandchild
        assert_eq!(db.size(), 1);
        assert!(!db.exists("root.child1"));
        assert!(!db.exists("root.child1.grandchild"));
        assert!(db.exists("root"));
    }

    #[test]
    fn test_lca() {
        let db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        let paths = vec![
            "root.a.b.c".to_string(),
            "root.a.b.d".to_string(),
            "root.a.e".to_string(),
        ];

        let lca = db.lca(&paths);
        assert_eq!(lca, Some("root.a".to_string()));

        let paths2 = vec![
            "root.a.b".to_string(),
            "other.x.y".to_string(),
        ];

        let lca2 = db.lca(&paths2);
        assert_eq!(lca2, None);
    }

    #[test]
    fn test_tree_stats() {
        let mut db = BasicConstructDB::new(
            "localhost".to_string(),
            5432,
            "test".to_string(),
            "user".to_string(),
            "pass".to_string(),
            "test_table".to_string(),
        );

        // Add test data
        let _ = db.store("root", serde_json::json!({"type": "root"}), None, None);
        let _ = db.store("root.child1", serde_json::json!({"type": "child"}), None, None);
        let _ = db.store("root.child2", serde_json::json!({"type": "child"}), None, None);
        let _ = db.store("root.child1.grandchild", serde_json::json!({"type": "grandchild"}), None, None);
        let _ = db.store("other_root", serde_json::json!({"type": "root"}), None, None);

        let stats = db.get_stats();
        assert_eq!(stats.total_nodes, 5);
        assert_eq!(stats.max_depth, 3);
        assert_eq!(stats.root_nodes, 2); // root and other_root
        assert_eq!(stats.leaf_nodes, 3); // child2, grandchild, other_root
    }
}

