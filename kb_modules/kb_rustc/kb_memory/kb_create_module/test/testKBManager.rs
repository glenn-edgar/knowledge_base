extern crate kb_create_module;

use kb_create_module::KnowledgeBaseManager;
use serde_json::json;
use std::env;

#[test]
fn test_kb_manager_basic_flow() {
    // Require POSTGRES_PASSWORD env var
    let password = env::var("POSTGRES_PASSWORD")
        .expect("Set POSTGRES_PASSWORD for tests");

    // Build connection string (uses test database)
    let conn_str = format!(
        "host=localhost user=gedgar dbname=knowledge_base_test password={} port=5432",
        password
    );

    // Use a distinct table prefix to isolate tests
    let table_prefix = "test_kb";

    // Initialize manager (connect + create tables)
    let mut mgr = KnowledgeBaseManager::new(table_prefix, &conn_str)
        .expect("Failed to initialize KB manager");

    // 1) Add knowledge bases
    mgr.add_kb("kb1", Some("First knowledge base"))
        .expect("add_kb failed");
    mgr.add_kb("kb2", Some("Second knowledge base"))
        .expect("add_kb failed");

    // 2) Add nodes
    mgr.add_node(
        "kb1",
        "person",
        "John Doe",
        Some(&json!({"age": 30})),
        Some(&json!({"email": "john@example.com"})),
        "people.john",
    )
    .expect("add_node failed");

    mgr.add_node(
        "kb2",
        "person",
        "Jane Smith",
        Some(&json!({"age": 25})),
        Some(&json!({"email": "jane@example.com"})),
        "people.jane",
    )
    .expect("add_node failed");

    // 3) Add a link mount and then a link
    mgr.add_link_mount(
        "kb1",
        "people.john",
        "link1",
        "link1 description",
    )
    .expect("add_link_mount failed");

    mgr.add_link("kb1", "people.john", "link1")
        .expect("add_link failed");

    // If we reach here without panicking, test passes
}
