CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS ltree;
-- Drop the table "kb_table" if it exists.
DROP TABLE IF EXISTS kb_table;
-- Create the table "kb_table" with the specified fields.
CREATE TABLE kb_table (
id SERIAL PRIMARY KEY, -- Unique id (auto-incrementing)
link_class TEXT, -- String field for link_class
link_type TEXT, -- String field for link_type
link_properties hstore, -- hstore field for link properties (key/value pairs)
node_class TEXT, -- String field for node_class
node_type TEXT, -- String field for node_type
node_properties hstore, -- hstore field for node properties (key/value pairs)
node_data JSON, -- JSON field for node data
node_path ltree -- ltree field for representing hierarchical paths
);

