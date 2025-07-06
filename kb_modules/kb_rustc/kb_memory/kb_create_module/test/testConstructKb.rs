// tests/integration_construct_kb.rs

use std::collections::HashMap;
use std::env;
use serde_json::json;
use kb_create_module::ConstructKb; // or your crate’s name

#[test]
fn test_construct_kb_flow() {
    // 1) Pull in credentials and test‐DB
    let password = env::var("POSTGRES_PASSWORD")
        .expect("Please set POSTGRES_PASSWORD for tests");
    let host  = "localhost";
    let port  = 5432;
    let db    = "knowledge_base_test";
    let user  = "gedgar";
    // use a unique table prefix for isolation
    let table = "test_construct_kb";

    let mut kb = ConstructKb::new(host, port, db, user, &password, table)
        .expect("failed to initialize ConstructKb");

    // === KB1 ===
    kb.add_kb("kb1", "First knowledge base").unwrap();
    kb.select_kb("kb1").unwrap();

    // header1
    let mut props = HashMap::new();
    props.insert("prop1".to_string(), json!("val1"));
    let data = json!({ "data": "header1_data" });
    kb.add_header_node("header1_link", "header1_name", &mut props, &data, None).unwrap();

    // info1
    let mut props = HashMap::new();
    props.insert("prop2".to_string(), json!("val2"));
    let data = json!({ "data": "info1_data" });
    kb.add_info_node("info1_link", "info1_name", &mut props, &data, None).unwrap();
    kb.leave_header_node("header1_link", "header1_name").unwrap();

    // header2/info2 + mount
    let mut props = HashMap::new();
    props.insert("prop3".to_string(), json!("val3"));
    let data = json!({ "data": "header2_data" });
    kb.add_header_node("header2_link", "header2_name", &mut props, &data, None).unwrap();

    let mut props = HashMap::new();
    props.insert("prop4".to_string(), json!("val4"));
    let data = json!({ "data": "info2_data" });
    kb.add_info_node("info2_link", "info2_name", &mut props, &data, None).unwrap();

    kb.add_link_mount("link1", Some("link1 description")).unwrap();
    kb.leave_header_node("header2_link", "header2_name").unwrap();

    // === KB2 ===
    kb.add_kb("kb2", "Second knowledge base").unwrap();
    kb.select_kb("kb2").unwrap();

    let mut props = HashMap::new();
    props.insert("prop1".to_string(), json!("val1"));
    let data = json!({ "data": "header1_data" });
    kb.add_header_node("header1_link", "header1_name", &mut props, &data, None).unwrap();

    let mut props = HashMap::new();
    props.insert("prop2".to_string(), json!("val2"));
    let data = json!({ "data": "info1_data" });
    kb.add_info_node("info1_link", "info1_name", &mut props, &data, None).unwrap();
    kb.leave_header_node("header1_link", "header1_name").unwrap();

    let mut props = HashMap::new();
    props.insert("prop3".to_string(), json!("val3"));
    let data = json!({ "data": "header2_data" });
    kb.add_header_node("header2_link", "header2_name", &mut props, &data, None).unwrap();

    let mut props = HashMap::new();
    props.insert("prop4".to_string(), json!("val4"));
    let data = json!({ "data": "info2_data" });
    kb.add_info_node("info2_link", "info2_name", &mut props, &data, None).unwrap();

    kb.add_link_node("link1").unwrap();
    kb.leave_header_node("header2_link", "header2_name").unwrap();

    // final check & teardown
    assert!(kb.check_installation().unwrap());
    kb.disconnect();
}
