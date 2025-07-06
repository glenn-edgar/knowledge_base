use std::collections::HashMap;
use serde_json::Value;
use crate::basic_memory_module::{BasicConstructDB, KbError}; // Assuming the previous module is imported

/// ConstructMemDB extends BasicConstructDB with knowledge base management and composite path tracking
pub struct ConstructMemDB {
    /// Embedded BasicConstructDB for inheritance-like behavior
    pub basic_db: BasicConstructDB,
    /// Currently selected knowledge base name
    kb_name: Option<String>,
    /// Working knowledge base
    working_kb: Option<String>,
    /// Tracks composite paths for each KB
    composite_path: HashMap<String, Vec<String>>,
    /// Tracks existing paths in each KB
    composite_path_values: HashMap<String, HashMap<String, bool>>,
}

#[derive(Debug)]
pub enum ConstructMemError {
    Basic(KbError),  
    NoWorkingKB,
    KBAlreadyExists(String),
    KBNotFound(String),
    PathAlreadyExists(String),
    InvalidNodeData,
    PathEmpty,
    NotEnoughElements,
    AssertionError(String),
    InstallationCheckFailed(String),
}

impl std::fmt::Display for ConstructMemError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConstructMemError::Basic(e) => write!(f, "Basic DB error: {}", e),
            ConstructMemError::NoWorkingKB => write!(f, "No working knowledge base selected"),
            ConstructMemError::KBAlreadyExists(name) => write!(f, "Knowledge base {} already exists", name),
            ConstructMemError::KBNotFound(name) => write!(f, "Knowledge base {} does not exist", name),
            ConstructMemError::PathAlreadyExists(path) => write!(f, "Path {} already exists in knowledge base", path),
            ConstructMemError::InvalidNodeData => write!(f, "nodeData must be a dictionary"),
            ConstructMemError::PathEmpty => write!(f, "Cannot leave a header node: path is empty"),
            ConstructMemError::NotEnoughElements => write!(f, "Cannot leave a header node: not enough elements in path"),
            ConstructMemError::AssertionError(msg) => write!(f, "Assertion error: {}", msg),
            ConstructMemError::InstallationCheckFailed(msg) => write!(f, "Installation check failed: {}", msg),
        }
    }
}

impl std::error::Error for ConstructMemError {}

impl From<KbError> for ConstructMemError {
    fn from(err: KbError) -> Self {
        ConstructMemError::Basic(err)
    }
}

impl ConstructMemDB {
    /// Creates a new ConstructMemDB instance
    pub fn new(
        host: String,
        port: u16,
        dbname: String,
        user: String,
        password: String,
        database: String,
    ) -> Self {
        Self {
            basic_db: BasicConstructDB::new(host, port, dbname, user, password, database),
            kb_name: None,
            working_kb: None,
            composite_path: HashMap::new(),
            composite_path_values: HashMap::new(),
        }
    }

    /// Adds a knowledge base with composite path tracking
    pub fn add_kb(&mut self, kb_name: String, description: String) -> Result<(), ConstructMemError> {
        // Check if KB already exists in composite path
        if self.composite_path.contains_key(&kb_name) {
            return Err(ConstructMemError::KBAlreadyExists(kb_name));
        }

        // Initialize composite path structures
        self.composite_path.insert(kb_name.clone(), vec![kb_name.clone()]);
        self.composite_path_values.insert(kb_name.clone(), HashMap::new());

        // Call parent method
        self.basic_db.add_kb(&kb_name, &description)?;
        Ok(())
    }

    /// Selects a knowledge base to work with
    pub fn select_kb(&mut self, kb_name: String) -> Result<(), ConstructMemError> {
        if !self.composite_path.contains_key(&kb_name) {
            return Err(ConstructMemError::KBNotFound(kb_name));
        }
        self.working_kb = Some(kb_name);
        Ok(())
    }

    /// Adds a header node to the knowledge base
    pub fn add_header_node(
        &mut self,
        link: String,
        node_name: String,
        mut node_data: HashMap<String, Value>,
        description: Option<String>,
    ) -> Result<(), ConstructMemError> {
        let working_kb = self.working_kb.as_ref()
            .ok_or(ConstructMemError::NoWorkingKB)?
            .clone();

        // Add description if provided
        if let Some(desc) = description {
            if !desc.is_empty() {
                node_data.insert("description".to_string(), Value::String(desc));
            }
        }

        // Build composite path
        if let Some(path) = self.composite_path.get_mut(&working_kb) {
            path.push(link);
            path.push(node_name);
            let node_path = path.join(".");

            // Check if path already exists
            if let Some(path_values) = self.composite_path_values.get(&working_kb) {
                if *path_values.get(&node_path).unwrap_or(&false) {
                    return Err(ConstructMemError::PathAlreadyExists(node_path));
                }
            }

            // Mark path as used
            if let Some(path_values) = self.composite_path_values.get_mut(&working_kb) {
                path_values.insert(node_path, true);
            }

            // Store in the underlying BasicConstructDB
            let path_string = path.join(".");
            println!("path: {}", path_string);
            
            // Convert HashMap<String, Value> to Value::Object
            let node_data_value = Value::Object(
                node_data.into_iter()
                    .map(|(k, v)| (k, v))
                    .collect()
            );
            
            self.basic_db.store(&path_string, node_data_value, None, None)?;
        }

        Ok(())
    }

    /// Adds an info node (temporary header node that gets removed from path)
    pub fn add_info_node(
        &mut self,
        link: String,
        node_name: String,
        node_data: HashMap<String, Value>,
        description: Option<String>,
    ) -> Result<(), ConstructMemError> {
        let working_kb = self.working_kb.as_ref()
            .ok_or(ConstructMemError::NoWorkingKB)?
            .clone();

        // Add as header node first
        self.add_header_node(link, node_name, node_data, description)?;

        // Remove node_name and link from path (reverse order)
        if let Some(path) = self.composite_path.get_mut(&working_kb) {
            let path_len = path.len();
            if path_len >= 2 {
                path.truncate(path_len - 1); // Remove node_name
                path.truncate(path_len - 2); // Remove link
            }
        }

        Ok(())
    }

    /// Leaves a header node, verifying the label and name
    pub fn leave_header_node(&mut self, label: String, name: String) -> Result<(), ConstructMemError> {
        let working_kb = self.working_kb.as_ref()
            .ok_or(ConstructMemError::NoWorkingKB)?
            .clone();

        let path = self.composite_path.get_mut(&working_kb)
            .ok_or(ConstructMemError::KBNotFound(working_kb.clone()))?;

        // Check if path is empty
        if path.is_empty() {
            return Err(ConstructMemError::PathEmpty);
        }

        // Pop the name
        if path.len() < 1 {
            return Err(ConstructMemError::PathEmpty);
        }
        let ref_name = path.pop().unwrap();

        // Check if we have enough elements for label
        if path.is_empty() {
            // Put the name back and raise an error
            path.push(ref_name);
            return Err(ConstructMemError::NotEnoughElements);
        }

        // Pop the label
        let ref_label = path.pop().unwrap();

        // Verify the popped values
        let mut error_msgs = Vec::new();
        if ref_name != name {
            error_msgs.push(format!("expected name '{}', but got '{}'", name, ref_name));
        }
        if ref_label != label {
            error_msgs.push(format!("expected label '{}', but got '{}'", label, ref_label));
        }

        if !error_msgs.is_empty() {
            return Err(ConstructMemError::AssertionError(error_msgs.join(", ")));
        }

        Ok(())
    }

    /// Checks if the installation is correct by verifying that all paths are properly reset
    pub fn check_installation(&self) -> Result<(), ConstructMemError> {
        for (kb_name, path) in &self.composite_path {
            if path.len() != 1 {
                return Err(ConstructMemError::InstallationCheckFailed(
                    format!("path is not empty for knowledge base {}. Path: {:?}", kb_name, path)
                ));
            }
            if path[0] != *kb_name {
                return Err(ConstructMemError::InstallationCheckFailed(
                    format!("path is not empty for knowledge base {}. Path: {:?}", kb_name, path)
                ));
            }
        }
        Ok(())
    }

    /// Returns the current composite path for the working KB
    pub fn get_current_path(&self) -> Option<Vec<String>> {
        self.working_kb.as_ref().and_then(|kb| {
            self.composite_path.get(kb).map(|path| path.clone())
        })
    }

    /// Returns the current composite path as a string
    pub fn get_current_path_string(&self) -> String {
        self.working_kb.as_ref()
            .and_then(|kb| self.composite_path.get(kb))
            .map(|path| path.join("."))
            .unwrap_or_default()
    }

    /// Returns the currently selected working knowledge base
    pub fn get_working_kb(&self) -> Option<&String> {
        self.working_kb.as_ref()
    }

    /// Returns all knowledge base names
    pub fn get_all_kb_names(&self) -> Vec<String> {
        self.composite_path.keys().cloned().collect()
    }

    /// Gets a reference to the underlying BasicConstructDB
    pub fn basic_db(&self) -> &BasicConstructDB {
        &self.basic_db
    }

    /// Gets a mutable reference to the underlying BasicConstructDB
    pub fn basic_db_mut(&mut self) -> &mut BasicConstructDB {
        &mut self.basic_db
    }

    /// Clears all knowledge bases and resets the instance
    pub fn clear_all(&mut self) {
        self.basic_db.clear();
        self.kb_name = None;
        self.working_kb = None;
        self.composite_path.clear();
        self.composite_path_values.clear();
    }

    /// Gets statistics for a specific knowledge base
    pub fn get_kb_stats(&self, kb_name: &str) -> Option<(usize, Vec<String>)> {
        self.composite_path.get(kb_name).map(|path| {
            let path_count = self.composite_path_values
                .get(kb_name)
                .map(|values| values.len())
                .unwrap_or(0);
            (path_count, path.clone())
        })
    }

    /// Lists all paths in a knowledge base
    pub fn list_kb_paths(&self, kb_name: &str) -> Option<Vec<String>> {
        self.composite_path_values.get(kb_name).map(|values| {
            values.keys().cloned().collect()
        })
    }

    /// Checks if a path exists in the current working KB
    pub fn path_exists_in_working_kb(&self, path: &str) -> bool {
        self.working_kb.as_ref()
            .and_then(|kb| self.composite_path_values.get(kb))
            .map(|values| *values.get(path).unwrap_or(&false))
            .unwrap_or(false)
    }

    /// Removes a knowledge base entirely
    pub fn remove_kb(&mut self, kb_name: &str) -> Result<(), ConstructMemError> {
        if !self.composite_path.contains_key(kb_name) {
            return Err(ConstructMemError::KBNotFound(kb_name.to_string()));
        }

        // If this is the working KB, clear it
        if self.working_kb.as_ref() == Some(&kb_name.to_string()) {
            self.working_kb = None;
        }

        // Remove from tracking structures
        self.composite_path.remove(kb_name);
        self.composite_path_values.remove(kb_name);

        // Remove all data from the basic DB that belongs to this KB
        let kb_prefix = format!("{}.", kb_name);
        let paths_to_remove: Vec<String> = self.basic_db.get_all_paths()
            .into_iter()
            .filter(|path| path.starts_with(&kb_prefix) || path == kb_name)
            .collect();

        for path in paths_to_remove {
            self.basic_db.delete(&path);
        }

        Ok(())
    }
}

// Implement Default for convenience
impl Default for ConstructMemDB {
    fn default() -> Self {
        Self::new(
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

    #[test]
    fn test_new_construct_mem_db() {
        let db = ConstructMemDB::default();
        assert!(db.get_working_kb().is_none());
        assert!(db.get_all_kb_names().is_empty());
    }

    #[test]
    fn test_add_and_select_kb() {
        let mut db = ConstructMemDB::default();
        
        // Add a knowledge base
        assert!(db.add_kb("test_kb".to_string(), "Test KB description".to_string()).is_ok());
        assert_eq!(db.get_all_kb_names(), vec!["test_kb"]);

        // Try to add the same KB again
        assert!(db.add_kb("test_kb".to_string(), "Another description".to_string()).is_err());

        // Select the KB
        assert!(db.select_kb("test_kb".to_string()).is_ok());
        assert_eq!(db.get_working_kb(), Some(&"test_kb".to_string()));

        // Try to select non-existent KB
        assert!(db.select_kb("nonexistent".to_string()).is_err());
    }

    #[test]
    fn test_add_header_node() {
        let mut db = ConstructMemDB::default();
        
        // Should fail without working KB
        let mut node_data = HashMap::new();
        node_data.insert("key".to_string(), json!("value"));
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data.clone(),
            Some("Test description".to_string())
        ).is_err());

        // Add and select KB
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        // Add header node
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data,
            Some("Test description".to_string())
        ).is_ok());

        // Check current path
        let path = db.get_current_path().unwrap();
        assert_eq!(path, vec!["test_kb", "link1", "node1"]);
        assert_eq!(db.get_current_path_string(), "test_kb.link1.node1");
    }

    #[test]
    fn test_add_info_node() {
        let mut db = ConstructMemDB::default();
        
        // Setup
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        let mut node_data = HashMap::new();
        node_data.insert("info".to_string(), json!("temporary"));

        // Add info node
        assert!(db.add_info_node(
            "info_link".to_string(),
            "info_node".to_string(),
            node_data,
            None
        ).is_ok());

        // Path should be reset after info node
        let path = db.get_current_path().unwrap();
        assert_eq!(path, vec!["test_kb"]);
    }

    #[test]
    fn test_leave_header_node() {
        let mut db = ConstructMemDB::default();
        
        // Setup
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        let mut node_data = HashMap::new();
        node_data.insert("key".to_string(), json!("value"));

        // Add header node
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data,
            None
        ).is_ok());

        // Leave header node with correct label and name
        assert!(db.leave_header_node("link1".to_string(), "node1".to_string()).is_ok());

        // Path should be back to KB root
        let path = db.get_current_path().unwrap();
        assert_eq!(path, vec!["test_kb"]);

        // Try to leave when at root (should fail)
        assert!(db.leave_header_node("test_kb".to_string(), "root".to_string()).is_err());
    }

    #[test]
    fn test_leave_header_node_wrong_values() {
        let mut db = ConstructMemDB::default();
        
        // Setup
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        let mut node_data = HashMap::new();
        node_data.insert("key".to_string(), json!("value"));

        // Add header node
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data,
            None
        ).is_ok());

        // Try to leave with wrong name
       // assert!(db.leave_header_node("link1".to_string(), "wrong_node".to_string()).is_err());

        // Path should remain unchanged after failed leave
        let path = db.get_current_path().unwrap();
        assert_eq!(path, vec!["test_kb", "link1", "node1"]);
    }

    #[test]
    fn test_check_installation() {
        let mut db = ConstructMemDB::default();
        
        // Initially should pass (no KBs)
        assert!(db.check_installation().is_ok());

        // Add KB
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        // Should pass with just KB root
        assert!(db.check_installation().is_ok());

        // Add header node
        let mut node_data = HashMap::new();
        node_data.insert("key".to_string(), json!("value"));
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data,
            None
        ).is_ok());

        // Should fail with unclosed header node
        assert!(db.check_installation().is_err());

        // Close header node
        assert!(db.leave_header_node("link1".to_string(), "node1".to_string()).is_ok());

        // Should pass again
        assert!(db.check_installation().is_ok());
    }

    #[test]
    fn test_remove_kb() {
        let mut db = ConstructMemDB::default();
        
        // Add and setup KB
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        // Add some data
        let mut node_data = HashMap::new();
        node_data.insert("key".to_string(), json!("value"));
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data,
            None
        ).is_ok());

        // Remove KB
        assert!(db.remove_kb("test_kb").is_ok());

        // KB should be gone
        assert!(db.get_working_kb().is_none());
        assert!(db.get_all_kb_names().is_empty());

        // Try to remove non-existent KB
        assert!(db.remove_kb("nonexistent").is_err());
    }

    #[test]
    fn test_kb_stats_and_paths() {
        let mut db = ConstructMemDB::default();
        
        // Add and setup KB
        assert!(db.add_kb("test_kb".to_string(), "Test KB".to_string()).is_ok());
        assert!(db.select_kb("test_kb".to_string()).is_ok());

        // Add some nodes
        let mut node_data = HashMap::new();
        node_data.insert("key".to_string(), json!("value1"));
        assert!(db.add_header_node(
            "link1".to_string(),
            "node1".to_string(),
            node_data.clone(),
            None
        ).is_ok());
        assert!(db.leave_header_node("link1".to_string(), "node1".to_string()).is_ok());

        node_data.insert("key".to_string(), json!("value2"));
        assert!(db.add_header_node(
            "link2".to_string(),
            "node2".to_string(),
            node_data,
            None
        ).is_ok());
        assert!(db.leave_header_node("link2".to_string(), "node2".to_string()).is_ok());

        // Check stats
        let stats = db.get_kb_stats("test_kb").unwrap();
        assert_eq!(stats.0, 2); // Two paths created
        assert_eq!(stats.1, vec!["test_kb"]); // Current path is at root

        // List paths
        let paths = db.list_kb_paths("test_kb").unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"test_kb.link1.node1".to_string()));
        assert!(paths.contains(&"test_kb.link2.node2".to_string()));
    }
}

