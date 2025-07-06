use std::collections::HashMap;
use serde_json::Value;
use crate::basic_memory_module::{BasicConstructDB, KbError, TreeNode};

/// SearchMemDB extends BasicConstructDB with search and filtering capabilities
pub struct SearchMemDB {
    /// Embedded BasicConstructDB for inheritance-like behavior
    pub basic_db: BasicConstructDB,
    /// Generated decoded keys
    keys: HashMap<String, Vec<String>>,
    /// Knowledge bases mapping
    kbs: HashMap<String, Vec<String>>,
    /// Labels mapping
    labels: HashMap<String, Vec<String>>,
    /// Names mapping
    names: HashMap<String, Vec<String>>,
    /// Decoded path keys
    pub decoded_keys: HashMap<String, Vec<String>>,
    /// Current filter results
    pub filter_results: HashMap<String, TreeNode>,
}

#[derive(Debug)]
pub enum SearchMemError {
    Basic(KbError),
    ImportFailed(String),
    QueryFailed(String),
}

impl std::fmt::Display for SearchMemError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SearchMemError::Basic(e) => write!(f, "Basic DB error: {}", e),
            SearchMemError::ImportFailed(msg) => write!(f, "Failed to import from postgres: {}", msg),
            SearchMemError::QueryFailed(msg) => write!(f, "Error querying: {}", msg),
        }
    }
}

impl std::error::Error for SearchMemError {}

impl From<KbError> for SearchMemError {
    fn from(err: KbError) -> Self {
        SearchMemError::Basic(err)
    }
}

impl SearchMemDB {
    /// Creates a new SearchMemDB instance and loads data from PostgreSQL
    pub async fn new(
        host: String,
        port: u16,
        dbname: String,
        user: String,
        password: String,
        table_name: String,
    ) -> Result<Self, SearchMemError> {
        let mut basic_db = BasicConstructDB::new(
            host, port, dbname, user, password, table_name.clone()
        );

        // Import data from PostgreSQL
        basic_db.import_from_postgres(&table_name, "path", "data", "created_at", "updated_at")
            .await
            .map_err(|e| SearchMemError::ImportFailed(e.to_string()))?;

        let mut smdb = Self {
            basic_db,
            keys: HashMap::new(),
            kbs: HashMap::new(),
            labels: HashMap::new(),
            names: HashMap::new(),
            decoded_keys: HashMap::new(),
            filter_results: HashMap::new(),
        };

        // Generate decoded keys
        smdb.generate_decoded_keys();
        
        // Initialize filter results with all data
        smdb.clear_filters();

        Ok(smdb)
    }

    /// Creates a new SearchMemDB instance without loading from PostgreSQL (for testing)
    pub fn new_empty(
        host: String,
        port: u16,
        dbname: String,
        user: String,
        password: String,
        table_name: String,
    ) -> Self {
        let basic_db = BasicConstructDB::new(
            host, port, dbname, user, password, table_name
        );

        let mut smdb = Self {
            basic_db,
            keys: HashMap::new(),
            kbs: HashMap::new(),
            labels: HashMap::new(),
            names: HashMap::new(),
            decoded_keys: HashMap::new(),
            filter_results: HashMap::new(),
        };

        smdb.generate_decoded_keys();
        smdb.clear_filters();
        smdb
    }

    /// Processes the data and creates lookup maps
    fn generate_decoded_keys(&mut self) {
        self.kbs.clear();
        self.labels.clear();
        self.names.clear();
        self.decoded_keys.clear();

        // Get all paths from the basic DB
        let all_paths = self.basic_db.get_all_paths();
        
        for key in all_paths {
            // Skip if we can't get the node
            if let Ok(Some(_node)) = self.basic_db.get_node(&key) {
                // Split the key into components
                let components: Vec<String> = key.split('.').map(|s| s.to_string()).collect();
                self.decoded_keys.insert(key.clone(), components.clone());
                
                if components.len() < 3 {
                    // Skip keys that don't have at least kb.label.name structure
                    continue;
                }

                let kb = &components[0];
                let label = &components[components.len() - 2];
                let name = &components[components.len() - 1];

                // Add to knowledge bases map
                self.kbs.entry(kb.clone()).or_insert_with(Vec::new).push(key.clone());

                // Add to labels map
                self.labels.entry(label.clone()).or_insert_with(Vec::new).push(key.clone());

                // Add to names map
                self.names.entry(name.clone()).or_insert_with(Vec::new).push(key.clone());
            }
        }

        self.keys = self.decoded_keys.clone();
    }

    /// Clears all filters and resets the query state
    #[allow(unused_variables)]
    pub fn clear_filters(&mut self) {
        self.filter_results.clear();
        
        // Copy all data to filter results
        let all_paths = self.basic_db.get_all_paths();
        for key in all_paths {
            if let Ok(Some(node)) = self.basic_db.get_node(&key) {
                self.filter_results.insert(key, node);
            }
        }
    }

    /// Searches for rows matching the specified knowledge base
    pub fn search_kb(&mut self, knowledge_base: &str) -> &HashMap<String, TreeNode> {
        let mut new_filter_results = HashMap::new();
        
        if let Some(kb_keys) = self.kbs.get(knowledge_base) {
            for key in kb_keys {
                if let Some(node) = self.filter_results.get(key) {
                    new_filter_results.insert(key.clone(), node.clone());
                }
            }
        }
        
        self.filter_results = new_filter_results;
        &self.filter_results
    }

    /// Searches for rows matching the specified label
    pub fn search_label(&mut self, label: &str) -> &HashMap<String, TreeNode> {
        let mut new_filter_results = HashMap::new();
        
        if let Some(label_keys) = self.labels.get(label) {
            for key in label_keys {
                if let Some(node) = self.filter_results.get(key) {
                    new_filter_results.insert(key.clone(), node.clone());
                }
            }
        }
        
        self.filter_results = new_filter_results;
        &self.filter_results
    }

    /// Searches for rows matching the specified name
    pub fn search_name(&mut self, name: &str) -> &HashMap<String, TreeNode> {
        let mut new_filter_results = HashMap::new();
        
        if let Some(name_keys) = self.names.get(name) {
            for key in name_keys {
                if let Some(node) = self.filter_results.get(key) {
                    new_filter_results.insert(key.clone(), node.clone());
                }
            }
        }
        
        self.filter_results = new_filter_results;
        &self.filter_results
    }

    /// Searches for rows that contain the specified property key
    pub fn search_property_key(&mut self, data_key: &str) -> &HashMap<String, TreeNode> {
        let mut new_filter_results = HashMap::new();
        
        for (key, node) in &self.filter_results {
            if let Value::Object(data_map) = &node.data {
                if data_map.contains_key(data_key) {
                    new_filter_results.insert(key.clone(), node.clone());
                }
            }
        }
        
        self.filter_results = new_filter_results;
        &self.filter_results
    }

    /// Searches for rows where the properties JSON field contains the specified key with the specified value
    pub fn search_property_value(&mut self, data_key: &str, data_value: &Value) -> &HashMap<String, TreeNode> {
        let mut new_filter_results = HashMap::new();
        
        for (key, node) in &self.filter_results {
            if let Value::Object(data_map) = &node.data {
                if let Some(value) = data_map.get(data_key) {
                    if value == data_value {
                        new_filter_results.insert(key.clone(), node.clone());
                    }
                }
            }
        }
        
        self.filter_results = new_filter_results;
        &self.filter_results
    }

    /// Searches for a specific path and all its descendants
    pub fn search_starting_path(&mut self, starting_path: &str) -> Result<&HashMap<String, TreeNode>, SearchMemError> {
        let mut new_filter_results = HashMap::new();
        
        // Add starting path if it exists in filter results
        if let Some(node) = self.filter_results.get(starting_path) {
            new_filter_results.insert(starting_path.to_string(), node.clone());
        } else {
            // If starting path doesn't exist, clear filter results
            self.filter_results.clear();
            return Ok(&self.filter_results);
        }
        
        
        // Get and add descendants
        let descendants = self.basic_db.query_descendants(starting_path)
            .map_err(|e| SearchMemError::QueryFailed(e.to_string()))?;

        for item in descendants {
            if let Some(node) = self.filter_results.get(&item.path) {
                new_filter_results.insert(item.path, node.clone());
            }
        }
        
        self.filter_results = new_filter_results;
        Ok(&self.filter_results)
    }

    

    /// Searches for rows matching the specified LTREE path expression using operators
    pub fn search_path<'a>(
        &mut self,
        operator: &'a str,
        starting_path: &'a str,
    ) -> &HashMap<String, TreeNode> {
        // Use the parent class query method
        let search_results = self.basic_db.query_by_operator(operator, starting_path,"");
        
        let mut new_filter_results = HashMap::new();
        for item in search_results {
            if let Some(node) = self.filter_results.get(&item.path) {
                new_filter_results.insert(item.path.clone(), node.clone());
            }
        }
        
        self.filter_results = new_filter_results;
        &self.filter_results
    }

    /// Extracts descriptions from all data entries
    pub fn find_descriptions(&self) -> HashMap<String, String> {
        let mut return_values = HashMap::new();
        
        // Process all data entries from basic_db
        let all_paths = self.basic_db.get_all_paths();
        for row_key in all_paths {
            if let Ok(Some(node)) = self.basic_db.get_node(&row_key) {
                if let Value::Object(data_map) = &node.data {
                    if let Some(Value::String(description)) = data_map.get("description") {
                        return_values.insert(row_key, description.clone());
                    } else {
                        return_values.insert(row_key, String::new());
                    }
                } else {
                    return_values.insert(row_key, String::new());
                }
            } else {
                return_values.insert(row_key, String::new());
            }
        }
        
        return_values
    }

    /// Returns the current filter results
    pub fn get_filter_results(&self) -> HashMap<String, TreeNode> {
        // Return a copy to prevent external modification
        self.filter_results.clone()
    }

    /// Returns just the keys of current filter results
    pub fn get_filter_result_keys(&self) -> Vec<String> {
        self.filter_results.keys().cloned().collect()
    }

    /// Returns all knowledge bases
    pub fn get_kbs(&self) -> &HashMap<String, Vec<String>> {
        &self.kbs
    }

    /// Returns all labels
    pub fn get_labels(&self) -> &HashMap<String, Vec<String>> {
        &self.labels
    }

    /// Returns all names
    pub fn get_names(&self) -> &HashMap<String, Vec<String>> {
        &self.names
    }

    /// Returns all decoded keys
    pub fn get_decoded_keys(&self) -> &HashMap<String, Vec<String>> {
        &self.decoded_keys
    }

    /// Gets a reference to the underlying BasicConstructDB
    pub fn basic_db(&self) -> &BasicConstructDB {
        &self.basic_db
    }

    /// Gets a mutable reference to the underlying BasicConstructDB
    pub fn basic_db_mut(&mut self) -> &mut BasicConstructDB {
        &mut self.basic_db
    }

    /// Refreshes the search indices after data changes
    pub fn refresh_indices(&mut self) {
        self.generate_decoded_keys();
        self.clear_filters();
    }

    /// Adds new data and refreshes indices
    pub fn add_data(&mut self, path: String, data: Value, created_at: Option<String>, updated_at: Option<String>) -> Result<(), SearchMemError> {
        self.basic_db.store(&path, data, created_at, updated_at)?;
        self.refresh_indices();
        Ok(())
    }

    /// Removes data and refreshes indices
    pub fn remove_data(&mut self, path: &str) -> bool {
        let result = self.basic_db.delete(path);
        if result {
            self.refresh_indices();
        }
        result
    }

    /// Gets the count of items in each category
    pub fn get_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        stats.insert("total_items".to_string(), self.basic_db.size());
        stats.insert("filter_results".to_string(), self.filter_results.len());
        stats.insert("knowledge_bases".to_string(), self.kbs.len());
        stats.insert("unique_labels".to_string(), self.labels.len());
        stats.insert("unique_names".to_string(), self.names.len());
        stats
    }

    /// Chains multiple search operations together
    pub fn chain_search(&mut self) -> SearchChain {
        SearchChain::new(self)
    }
}

/// Helper struct for chaining search operations
pub struct SearchChain<'a> {
    search_db: &'a mut SearchMemDB,
}

impl<'a> SearchChain<'a> {
    fn new(search_db: &'a mut SearchMemDB) -> Self {
        Self { search_db }
    }

    /// Chain a KB search
    pub fn kb(self, knowledge_base: &str) -> Self {
        self.search_db.search_kb(knowledge_base);
        self
    }

    /// Chain a label search
    pub fn label(self, label: &str) -> Self {
        self.search_db.search_label(label);
        self
    }

    /// Chain a name search
    pub fn name(self, name: &str) -> Self {
        self.search_db.search_name(name);
        self
    }

    /// Chain a property key search
    pub fn property_key(self, data_key: &str) -> Self {
        self.search_db.search_property_key(data_key);
        self
    }

    /// Chain a property value search
    pub fn property_value(self, data_key: &str, data_value: &Value) -> Self {
        self.search_db.search_property_value(data_key, data_value);
        self
    }

    /// Chain a path search
    pub fn path(self, operator: &str, starting_path: &str) -> Self {
        self.search_db.search_path(operator, starting_path);
        self
    }

    /// Get the final results
    pub fn results(self) -> &'a HashMap<String, TreeNode> {
        &self.search_db.filter_results
    }

    /// Get the final result keys
    pub fn result_keys(self) -> Vec<String> {
        self.search_db.get_filter_result_keys()
    }
}

// Implement Default for convenience
impl Default for SearchMemDB {
    fn default() -> Self {
        Self::new_empty(
            "localhost".to_string(),
            5432,
            "postgres".to_string(),
            "postgres".to_string(),
            "".to_string(),
            "ltree_data".to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_db() -> SearchMemDB {
        let mut db = SearchMemDB::default();
        /*
        // for test starting path
        let _ = db.add_data(
            "kb1.section1".to_string(),
            json!({"description": "Top item", "category": "Top"}),
            None, None
        );
        */
        let _ = db.add_data(
            "kb1.section1.item2".to_string(),
            json!({"description": "Second item", "category": "B"}),
            None, None
        );
        let _ = db.add_data(
            "kb1.section1.item1".to_string(),
            json!({"description": "First item", "category": "A"}),
            None, None
        );
        let _ = db.add_data(
            "kb1.section1.item2".to_string(),
            json!({"description": "Second item", "category": "B"}),
            None, None
        );
        let _ = db.add_data(
            "kb1.section2.item3".to_string(),
            json!({"description": "Third item", "category": "A"}),
            None, None
        );
        let _ = db.add_data(
            "kb2.section1.item4".to_string(),
            json!({"description": "Fourth item", "category": "C"}),
            None, None
        );
        
        db
    }

    #[test]
    fn test_new_search_mem_db() {
        let db = SearchMemDB::default();
        assert_eq!(db.get_stats()["total_items"], 0);
    }

    #[test]
    fn test_add_data_and_indices() {
        let mut db = SearchMemDB::default();
        
        assert!(db.add_data(
            "test_kb.test_label.test_name".to_string(),
            json!({"description": "Test item"}),
            None, None
        ).is_ok());

        // Check that indices were updated
        assert!(db.get_kbs().contains_key("test_kb"));
        assert!(db.get_labels().contains_key("test_label"));
        assert!(db.get_names().contains_key("test_name"));
        assert_eq!(db.get_stats()["total_items"], 1);
    }

    #[test]
    fn test_search_kb() {
        let mut db = create_test_db();
        
        // Search for kb1
        let results = db.search_kb("kb1");
        assert_eq!(results.len(), 3);
        
        // All results should be from kb1
        for key in results.keys() {
            assert!(key.starts_with("kb1."));
        }
    }

    #[test]
    fn test_search_label() {
        let mut db = create_test_db();
        
        // Search for section1
        let results = db.search_label("section1");
        assert_eq!(results.len(), 3);
        
        // All results should contain section1
        for key in results.keys() {
            assert!(key.contains(".section1."));
        }
    }

    #[test]
    fn test_search_name() {
        let mut db = create_test_db();
        
        // Search for item1
        let results = db.search_name("item1");
        assert_eq!(results.len(), 1);
        assert!(results.contains_key("kb1.section1.item1"));
    }

    #[test]
    fn test_search_property_key() {
        let mut db = create_test_db();
        
        // Search for items with "category" property
        let results = db.search_property_key("category");
        assert_eq!(results.len(), 4); // All items have category
        
        // Search for non-existent property
        db.clear_filters();
        let results = db.search_property_key("nonexistent");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_search_property_value() {
        let mut db = create_test_db();
        
        // Search for items with category "A"
        let results = db.search_property_value("category", &json!("A"));
        assert_eq!(results.len(), 2);
        
        // Verify the results
        assert!(results.contains_key("kb1.section1.item1"));
        assert!(results.contains_key("kb1.section2.item3"));
    }
    /*
    #[test]
    fn test_search_starting_path() {
        let mut db = create_test_db();
        
        // Search for kb1 and all descendants
        let results = db.search_starting_path("kb1.section1").unwrap();
        
        assert_eq!(results.len(), 3); // Only descendants,"kb1.section1" is starting path
    }
    */
    #[test]
    fn test_chained_search() {
        let mut db = create_test_db();
        
        // Chain multiple searches: kb1 -> section1 -> category A
        let results = db.chain_search()
            .kb("kb1")
            .label("section1")
            .property_value("category", &json!("A"))
            .results();
        
        assert_eq!(results.len(), 1);
        assert!(results.contains_key("kb1.section1.item1"));
    }

    #[test]
    fn test_clear_filters() {
        let mut db = create_test_db();
        
        // Apply a filter
        db.search_kb("kb1");
        assert_eq!(db.filter_results.len(), 3);
        
        // Clear filters
        db.clear_filters();
        assert_eq!(db.filter_results.len(), 4); // Back to all items
    }

    #[test]
    fn test_find_descriptions() {
        let db = create_test_db();
        
        let descriptions = db.find_descriptions();
        assert_eq!(descriptions.len(), 4);
        assert_eq!(descriptions["kb1.section1.item1"], "First item");
        assert_eq!(descriptions["kb1.section1.item2"], "Second item");
    }

    #[test]
    fn test_remove_data() {
        let mut db = create_test_db();
        
        assert_eq!(db.get_stats()["total_items"], 4);
        
        // Remove an item
        assert!(db.remove_data("kb1.section1.item1"));
        assert_eq!(db.get_stats()["total_items"], 3);
        
        // Verify indices were updated
        let results = db.search_name("item1");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_get_stats() {
        let db = create_test_db();
        
        let stats = db.get_stats();
        assert_eq!(stats["total_items"], 4);
        assert_eq!(stats["filter_results"], 4);
        assert_eq!(stats["knowledge_bases"], 2);
        assert!(stats["unique_labels"] >= 2);
        assert!(stats["unique_names"] >= 4);
    }
}


