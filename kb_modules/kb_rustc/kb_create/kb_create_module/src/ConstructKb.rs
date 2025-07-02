// src/construct_kb.rs

use std::collections::HashMap;
use serde_json::Value;
use postgres::Error;
use base_construct_kb::KnowledgeBaseManager;

/// Builds on KnowledgeBaseManager to maintain header/info stacks per KB.
pub struct ConstructKb {
    inner: KnowledgeBaseManager,
    /// path stack for each KB (e.g. ["kb1", "link1", "node1", …])
    pub path: HashMap<String, Vec<String>>,
    /// tracks which full paths have been used per KB
    pub path_values: HashMap<String, HashMap<String, bool>>,
    working_kb: Option<String>,
}

impl ConstructKb {
    /// Connects and (re)creates tables, just like Python __init__.
    pub fn new(
        host: &str,
        port: u16,
        dbname: &str,
        user: &str,
        password: &str,
        table_name: &str,
    ) -> Result<Self, Error> {
        let conn_str = format!(
            "host={} port={} dbname={} user={} password={}",
            host, port, dbname, user, password
        );
        let mgr = KnowledgeBaseManager::new(table_name, &conn_str)?;
        Ok(Self {
            inner: mgr,
            path: HashMap::new(),
            path_values: HashMap::new(),
            working_kb: None,
        })
    }

    /// Expose the raw client & transaction if you need it.
    pub fn get_db_objects(&mut self) -> (&mut postgres::Client, &mut postgres::Transaction) {
        self.inner.get_db_objects()
    }

    /// Mirror Python add_kb: initialize stacks and call base.
    pub fn add_kb(&mut self, kb_name: &str, description: &str) -> Result<(), Error> {
        if self.path.contains_key(kb_name) {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("KB '{}' already exists", kb_name),
            )));
        }
        self.inner.add_kb(kb_name, Some(description))?;
        self.path.insert(kb_name.to_string(), vec![kb_name.to_string()]);
        self.path_values.insert(kb_name.to_string(), HashMap::new());
        Ok(())
    }

    /// Select which KB subsequent calls apply to.
    pub fn select_kb(&mut self, kb_name: &str) -> Result<(), Error> {
        if !self.path.contains_key(kb_name) {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("KB '{}' does not exist", kb_name),
            )));
        }
        self.working_kb = Some(kb_name.to_string());
        Ok(())
    }

    /// Push a header node, record it, and call base.add_node.
    pub fn add_header_node(
        &mut self,
        link: &str,
        node_name: &str,
        node_properties: &mut HashMap<String, Value>,
        node_data: &Value,
        description: Option<&str>,
    ) -> Result<(), Error> {
        let kb = self.working_kb.as_ref().ok_or_else(|| {
            Error::from(std::io::Error::new(std::io::ErrorKind::InvalidInput, "No KB selected"))
        })?;

        // optionally attach description
        if let Some(desc) = description {
            node_properties.insert("description".into(), Value::String(desc.into()));
        }

        let stack = self.path.get_mut(kb).unwrap();
        stack.push(link.into());
        stack.push(node_name.into());
        let full_path = stack.join(".");

        let vals = self.path_values.get_mut(kb).unwrap();
        if vals.contains_key(&full_path) {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Path '{}' already exists", full_path),
            )));
        }
        vals.insert(full_path.clone(), true);

        self.inner.add_node(
            kb,
            link,
            node_name,
            Some(&Value::Object(node_properties.clone())),
            Some(node_data),
            &full_path,
        )?;
        Ok(())
    }

    /// Add header node then immediately pop it off.
    pub fn add_info_node(
        &mut self,
        link: &str,
        node_name: &str,
        node_properties: &mut HashMap<String, Value>,
        node_data: &Value,
        description: Option<&str>,
    ) -> Result<(), Error> {
        self.add_header_node(link, node_name, node_properties, node_data, description)?;
        let kb = self.working_kb.as_ref().unwrap();
        let stack = self.path.get_mut(kb).unwrap();
        stack.pop();
        stack.pop();
        Ok(())
    }

    /// Pop a header, verifying it matches the expected label & name.
    pub fn leave_header_node(&mut self, label: &str, name: &str) -> Result<(), Error> {
        let kb = self.working_kb.as_ref().ok_or_else(|| {
            Error::from(std::io::Error::new(std::io::ErrorKind::InvalidInput, "No KB selected"))
        })?;
        let stack = self.path.get_mut(kb).unwrap();
        if stack.len() < 3 {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Not enough elements to leave header",
            )));
        }
        let popped_name = stack.pop().unwrap();
        let popped_label = stack.pop().unwrap();
        if popped_name != name || popped_label != label {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Expected label/name '{}/{}' but got '{}/{}'",
                    label, name, popped_label, popped_name
                ),
            )));
        }
        Ok(())
    }

    /// Create a link under the current header path.
    pub fn add_link_node(&mut self, link_name: &str) -> Result<(), Error> {
        let kb = self.working_kb.as_ref().unwrap();
        let full_path = self.path.get(kb).unwrap().join(".");
        self.inner.add_link(kb, &full_path, link_name)
    }

    /// Create a link‐mount under the current header path.
    pub fn add_link_mount(
        &mut self,
        link_mount_name: &str,
        description: Option<&str>,
    ) -> Result<(), Error> {
        let kb = self.working_kb.as_ref().unwrap();
        let full_path = self.path.get(kb).unwrap().join(".");
        self.inner
            .add_link_mount(kb, &full_path, link_mount_name, description.unwrap_or(""))
            .map(|_| ())
    }

    /// Verify all stacks are reset to just the KB name.
    pub fn check_installation(&self) -> Result<bool, Error> {
        for (kb, stack) in &self.path {
            if stack.len() != 1 || stack[0] != *kb {
                return Err(Error::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Installation check failed for KB {:?}: {:?}", kb, stack),
                )));
            }
        }
        Ok(true)
    }

    /// Cleanly close the connection.
    pub fn disconnect(self) {
        self.inner.disconnect();
    }
}
