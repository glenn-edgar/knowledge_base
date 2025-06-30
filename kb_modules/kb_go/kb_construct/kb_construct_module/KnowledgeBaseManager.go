package kb_construct_module

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

// KnowledgeBaseManager manages the knowledge base in a PostgreSQL database.
type KnowledgeBaseManager struct {
	conn           *sql.DB
	tableName      string
	connParams     map[string]interface{}
}

// NewKnowledgeBaseManager initializes a new KnowledgeBaseManager.
func NewKnowledgeBaseManager(tableName string, connParams map[string]interface{}) (*KnowledgeBaseManager, error) {
	kb := &KnowledgeBaseManager{
		tableName:  tableName,
		connParams: connParams,
	}

	if err := kb.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := kb.createTables(); err != nil {
		kb.disconnect()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return kb, nil
}

// connect establishes a database connection.
func (kb *KnowledgeBaseManager) connect() error {
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		kb.connParams["host"],
		int(kb.connParams["port"].(float64)),
		kb.connParams["database"],
		kb.connParams["user"],
		kb.connParams["password"],
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error opening connection: %w", err)
	}

	kb.conn = db

	// Enable ltree extension
	_, err = db.Exec("CREATE EXTENSION IF NOT EXISTS ltree;")
	if err != nil {
		db.Close()
		return fmt.Errorf("error enabling ltree extension: %w", err)
	}

	return nil
}

// disconnect closes the database connection.
func (kb *KnowledgeBaseManager) disconnect() {
	if kb.conn != nil {
		kb.conn.Close()
	}
}

// deleteTable deletes a specified table from the database.
func (kb *KnowledgeBaseManager) deleteTable(tableName, schema string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", schema, tableName)
	_, err := kb.conn.Exec(query)
	if err != nil {
		return fmt.Errorf("error deleting table %s.%s: %w", schema, tableName, err)
	}
	return nil
}

// createTables creates the knowledge base tables and indexes.
func (kb *KnowledgeBaseManager) createTables() error {
	tx, err := kb.conn.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete existing tables
	tables := []string{
		kb.tableName,
		kb.tableName + "_info",
		kb.tableName + "_link",
		kb.tableName + "_link_mount",
	}
	for _, table := range tables {
		if err := kb.deleteTable(table, "public"); err != nil {
			return err
		}
	}

	// Create knowledge base table
	kbTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			knowledge_base VARCHAR NOT NULL,
			label VARCHAR NOT NULL,
			name VARCHAR NOT NULL,
			properties JSON,
			data JSON,
			has_link BOOLEAN DEFAULT FALSE,
			has_link_mount BOOLEAN DEFAULT FALSE,
			path LTREE UNIQUE
		)`, kb.tableName)
	if _, err := tx.Exec(kbTableQuery); err != nil {
		return fmt.Errorf("error creating table %s: %w", kb.tableName, err)
	}

	// Create info table
	infoTableQuery := fmt.Sprintf(`
		CREATE TABLE %s_info (
			id SERIAL PRIMARY KEY,
			knowledge_base VARCHAR NOT NULL UNIQUE,
			description VARCHAR
		)`, kb.tableName)
	if _, err := tx.Exec(infoTableQuery); err != nil {
		return fmt.Errorf("error creating table %s_info: %w", kb.tableName, err)
	}

	// Create link table
	linkTableQuery := fmt.Sprintf(`
		CREATE TABLE %s_link (
			id SERIAL PRIMARY KEY,
			link_name VARCHAR NOT NULL,
			parent_node_kb VARCHAR NOT NULL,
			parent_path LTREE NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(link_name, parent_node_kb, parent_path)
		)`, kb.tableName)
	if _, err := tx.Exec(linkTableQuery); err != nil {
		return fmt.Errorf("error creating table %s_link: %w", kb.tableName, err)
	}

	// Create link mount table
	linkMountTableQuery := fmt.Sprintf(`
		CREATE TABLE %s_link_mount (
			id SERIAL PRIMARY KEY,
			link_name VARCHAR NOT NULL UNIQUE,
			knowledge_base VARCHAR NOT NULL,
			mount_path LTREE NOT NULL,
			description VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(knowledge_base, mount_path)
		)`, kb.tableName)
	if _, err := tx.Exec(linkMountTableQuery); err != nil {
		return fmt.Errorf("error creating table %s_link_mount: %w", kb.tableName, err)
	}

	// Create indexes
	indexes := []string{
		// Main table indexes
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_kb ON %s (knowledge_base)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_path ON %s USING GIST (path)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_label ON %s (label)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_name ON %s (name)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_has_link ON %s (has_link)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_has_link_mount ON %s (has_link_mount)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_kb_path ON %s (knowledge_base, path)`, kb.tableName, kb.tableName),
		// Info table indexes
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_info_kb ON %s_info (knowledge_base)`, kb.tableName, kb.tableName),
		// Link table indexes
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_link_name ON %s_link (link_name)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_link_parent_kb ON %s_link (parent_node_kb)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_link_parent_path ON %s_link USING GIST (parent_path)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_link_created ON %s_link (created_at)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_link_composite ON %s_link (link_name, parent_node_kb)`, kb.tableName, kb.tableName),
		// Mount table indexes
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_mount_link_name ON %s_link_mount (link_name)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_mount_kb ON %s_link_mount (knowledge_base)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_mount_path ON %s_link_mount USING GIST (mount_path)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_mount_created ON %s_link_mount (created_at)`, kb.tableName, kb.tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_mount_composite ON %s_link_mount (knowledge_base, mount_path)`, kb.tableName, kb.tableName),
	}

	for _, indexQuery := range indexes {
		if _, err := tx.Exec(indexQuery); err != nil {
			return fmt.Errorf("error creating index: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// AddKB adds a knowledge base entry to the info table.
func (kb *KnowledgeBaseManager) AddKB(kbName, description string) error {
	if kbName == "" {
		return fmt.Errorf("kb_name must be a non-empty string")
	}
	if description != "" && !strings.Contains(description, "") {
		return fmt.Errorf("description must be a valid string")
	}

	tx, err := kb.conn.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
		INSERT INTO %s_info (knowledge_base, description)
		VALUES ($1, $2)
		ON CONFLICT (knowledge_base) DO NOTHING
	`, kb.tableName)
	_, err = tx.Exec(query, kbName, description)
	if err != nil {
		return fmt.Errorf("error adding knowledge base: %w", err)
	}

	return tx.Commit()
}

// AddNode adds a node to the knowledge base.
func (kb *KnowledgeBaseManager) AddNode(kbName, label, name string, properties, data map[string]interface{}, path string) error {
	if kbName == "" || label == "" || name == "" {
		return fmt.Errorf("kb_name, label, and name must be non-empty strings")
	}
	if path == "" {
		return fmt.Errorf("path must be a non-empty string")
	}

	tx, err := kb.conn.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if kb_name exists in info table
	checkQuery := fmt.Sprintf(`SELECT 1 FROM %s_info WHERE knowledge_base = $1`, kb.tableName)
	var exists int
	err = tx.QueryRow(checkQuery, kbName).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("knowledge base '%s' not found in info table", kbName)
	} else if err != nil {
		return fmt.Errorf("error checking knowledge base: %w", err)
	}

	// Convert maps to JSON
	var propertiesJSON, dataJSON []byte
	if properties != nil {
		propertiesJSON, err = json.Marshal(properties)
		if err != nil {
			return fmt.Errorf("error marshaling properties: %w", err)
		}
	}
	if data != nil {
		dataJSON, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("error marshaling data: %w", err)
		}
	}

	// Insert node
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (knowledge_base, label, name, properties, data, has_link, path)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, kb.tableName)
	_, err = tx.Exec(insertQuery, kbName, label, name, propertiesJSON, dataJSON, false, path)
	if err != nil {
		return fmt.Errorf("error adding node: %w", err)
	}

	return tx.Commit()
}

// AddLink adds a link between two nodes in the knowledge base.
func (kb *KnowledgeBaseManager) AddLink(parentKB, parentPath, linkName string) error {
	if parentKB == "" || parentPath == "" || linkName == "" {
		return fmt.Errorf("parent_kb, parent_path, and link_name must be non-empty strings")
	}

	tx, err := kb.conn.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if parent knowledge base exists
	checkKBQuery := fmt.Sprintf(`SELECT knowledge_base FROM %s_info WHERE knowledge_base = $1`, kb.tableName)
	var foundKB string
	err = tx.QueryRow(checkKBQuery, parentKB).Scan(&foundKB)
	if err == sql.ErrNoRows {
		return fmt.Errorf("parent knowledge base '%s' not found", parentKB)
	} else if err != nil {
		return fmt.Errorf("error checking knowledge base: %w", err)
	}

	// Check if parent node exists
	checkNodeQuery := fmt.Sprintf(`SELECT path FROM %s WHERE path = $1`, kb.tableName)
	var path string
	err = tx.QueryRow(checkNodeQuery, parentPath).Scan(&path)
	if err == sql.ErrNoRows {
		return fmt.Errorf("parent node with path '%s' not found", parentPath)
	} else if err != nil {
		return fmt.Errorf("error checking node: %w", err)
	}

	// Check if link name exists in link_mount table (it SHOULD exist)
	checkLinkQuery := fmt.Sprintf(`SELECT link_name FROM %s_link_mount WHERE link_name = $1`, kb.tableName)
	var existingLink string
	err = tx.QueryRow(checkLinkQuery, linkName).Scan(&existingLink)
	if err == sql.ErrNoRows {
		return fmt.Errorf("link name '%s' not found in link_mount table", linkName)
	} else if err != nil {
		return fmt.Errorf("error checking link name: %w", err)
	}

	// Insert link
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s_link (parent_node_kb, parent_path, link_name)
		VALUES ($1, $2, $3)
	`, kb.tableName)
	_, err = tx.Exec(insertQuery, parentKB, parentPath, linkName)
	if err != nil {
		return fmt.Errorf("error adding link: %w", err)
	}

	// Update has_link flag
	updateQuery := fmt.Sprintf(`UPDATE %s SET has_link = TRUE WHERE path = $1`, kb.tableName)
	_, err = tx.Exec(updateQuery, parentPath)
	if err != nil {
		return fmt.Errorf("error updating has_link: %w", err)
	}

	return tx.Commit()
}

// AddLinkMount adds a link mount to the knowledge base.
func (kb *KnowledgeBaseManager) AddLinkMount(knowledgeBase, path, linkMountName, description string) (string, string, error) {
	if knowledgeBase == "" || path == "" || linkMountName == "" {
		return "", "", fmt.Errorf("knowledge_base, path, and link_mount_name must be non-empty strings")
	}

	tx, err := kb.conn.Begin()
	if err != nil {
		return "", "", fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Verify knowledge base exists
	checkKBQuery := fmt.Sprintf(`SELECT knowledge_base FROM %s_info WHERE knowledge_base = $1`, kb.tableName)
	var foundKB string
	err = tx.QueryRow(checkKBQuery, knowledgeBase).Scan(&foundKB)
	if err == sql.ErrNoRows {
		return "", "", fmt.Errorf("knowledge base '%s' does not exist in info table", knowledgeBase)
	} else if err != nil {
		return "", "", fmt.Errorf("error checking knowledge base: %w", err)
	}

	// Verify path exists
	checkPathQuery := fmt.Sprintf(`SELECT id FROM %s WHERE knowledge_base = $1 AND path = $2`, kb.tableName)
	var id int
	err = tx.QueryRow(checkPathQuery, knowledgeBase, path).Scan(&id)
	if err == sql.ErrNoRows {
		return "", "", fmt.Errorf("path '%s' does not exist for knowledge base '%s'", path, knowledgeBase)
	} else if err != nil {
		return "", "", fmt.Errorf("error checking path: %w", err)
	}

	// Verify link name does not exist
	checkLinkQuery := fmt.Sprintf(`SELECT link_name FROM %s_link_mount WHERE link_name = $1`, kb.tableName)
	var existingLink string
	err = tx.QueryRow(checkLinkQuery, linkMountName).Scan(&existingLink)
	if err == nil {
		return "", "", fmt.Errorf("link name '%s' already exists in line table", linkMountName)
	} else if err != sql.ErrNoRows {
		return "", "", fmt.Errorf("error checking link name: %w", err)
	}

	// Insert link mount
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s_link_mount (link_name, knowledge_base, mount_path, description)
		VALUES ($1, $2, $3, $4)
	`, kb.tableName)
	result, err := tx.Exec(insertQuery, linkMountName, knowledgeBase, path, description)
	if err != nil {
		return "", "", fmt.Errorf("error inserting link mount: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return "", "", fmt.Errorf("failed to insert link mount with link_name '%s'", linkMountName)
	}

	// Update has_link_mount flag
	updateQuery := fmt.Sprintf(`
		UPDATE %s SET has_link_mount = TRUE 
		WHERE knowledge_base = $1 AND path = $2
	`, kb.tableName)
	result, err = tx.Exec(updateQuery, knowledgeBase, path)
	if err != nil {
		return "", "", fmt.Errorf("error updating has_link_mount: %w", err)
	}
	rowsAffected, _ = result.RowsAffected()
	if rowsAffected == 0 {
		return "", "", fmt.Errorf("no rows updated for knowledge_base '%s' and path '%s'", knowledgeBase, path)
	}

	if err := tx.Commit(); err != nil {
		return "", "", fmt.Errorf("error committing transaction: %w", err)
	}

	return knowledgeBase, path, nil
}

func main() {
	// Example usage
	connParams := map[string]interface{}{
		"host":     "localhost",
		"port":     5432,
		"database": "knowledge_base",
		"user":     "gedgar",
		"password": "your_password_here", // Replace with actual password
	}

	kbManager, err := NewKnowledgeBaseManager("knowledge_base", connParams)
	if err != nil {
		log.Fatalf("Error initializing KnowledgeBaseManager: %v", err)
	}
	defer kbManager.disconnect()

	fmt.Println("Starting unit test")

	// Add knowledge bases
	if err := kbManager.AddKB("kb1", "First knowledge base"); err != nil {
		log.Fatalf("Error adding kb1: %v", err)
	}
	if err := kbManager.AddKB("kb2", "Second knowledge base"); err != nil {
		log.Fatalf("Error adding kb2: %v", err)
	}

	// Add nodes
	properties1 := map[string]interface{}{"age": 30}
	data1 := map[string]interface{}{"email": "john@example.com"}
	if err := kbManager.AddNode("kb1", "person", "John Doe", properties1, data1, "people.john"); err != nil {
		log.Fatalf("Error adding node John Doe: %v", err)
	}

	properties2 := map[string]interface{}{"age": 25}
	data2 := map[string]interface{}{"email": "jane@example.com"}
	if err := kbManager.AddNode("kb2", "person", "Jane Smith", properties2, data2, "people.jane"); err != nil {
		log.Fatalf("Error adding node Jane Smith: %v", err)
	}

	// Add link mount
	if _, _, err := kbManager.AddLinkMount("kb1", "people.john", "link1", "link1 description"); err != nil {
		log.Fatalf("Error adding link mount: %v", err)
	}

	// Add link
	if err := kbManager.AddLink("kb1", "people.john", "link1"); err != nil {
		log.Fatalf("Error adding link: %v", err)
	}

	fmt.Println("Ending unit test")
}