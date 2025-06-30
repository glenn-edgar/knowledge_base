
package kb_construct_module

import (
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"kb_construct_module"
)

// TestKnowledgeBaseManager tests the core functionality of KnowledgeBaseManager.
func TestKnowledgeBaseManager(t *testing.T) {
	// Retrieve password from environment variable
	password := os.Getenv("PG_PASSWORD")
	if password == "" {
		t.Fatal("PG_PASSWORD environment variable not set")
	}

	// Define connection parameters
	connParams := map[string]interface{}{
		"host":     "localhost",
		"port":     5432,
		"database": "knowledge_base",
		"user":     "gedgar",
		"password": password,
	}

	// Initialize KnowledgeBaseManager
	kbManager, err := NewKnowledgeBaseManager("knowledge_base", connParams)
	if err != nil {
		t.Fatalf("Error initializing KnowledgeBaseManager: %v", err)
	}
	defer kbManager.disconnect()

	// Test adding knowledge bases
	t.Run("AddKB", func(t *testing.T) {
		t.Log("Adding knowledge base kb1...")
		if err := kbManager.AddKB("kb1", "First knowledge base"); err != nil {
			t.Fatalf("Error adding kb1: %v", err)
		}
		t.Log("Successfully added kb1")

		t.Log("Adding knowledge base kb2...")
		if err := kbManager.AddKB("kb2", "Second knowledge base"); err != nil {
			t.Fatalf("Error adding kb2: %v", err)
		}
		t.Log("Successfully added kb2")
	})

	// Test adding nodes
	t.Run("AddNode", func(t *testing.T) {
		t.Log("Adding node John Doe...")
		properties1 := map[string]interface{}{"age": 30}
		data1 := map[string]interface{}{"email": "john@example.com"}
		if err := kbManager.AddNode("kb1", "person", "John Doe", properties1, data1, "people.john"); err != nil {
			t.Fatalf("Error adding node John Doe: %v", err)
		}
		t.Log("Successfully added node John Doe")

		t.Log("Adding node Jane Smith...")
		properties2 := map[string]interface{}{"age": 25}
		data2 := map[string]interface{}{"email": "jane@example.com"}
		if err := kbManager.AddNode("kb2", "person", "Jane Smith", properties2, data2, "people.jane"); err != nil {
			t.Fatalf("Error adding node Jane Smith: %v", err)
		}
		t.Log("Successfully added node Jane Smith")
	})

	// Test adding link mount
	t.Run("AddLinkMount", func(t *testing.T) {
		t.Log("Adding link mount link1...")
		kb, path, err := kbManager.AddLinkMount("kb1", "people.john", "link1", "link1 description")
		if err != nil {
			t.Fatalf("Error adding link mount: %v", err)
		}
		if kb != "kb1" || path != "people.john" {
			t.Errorf("AddLinkMount returned unexpected values: got (%s, %s), want (kb1, people.john)", kb, path)
		}
		t.Log("Successfully added link mount link1")
	})

	// Test adding link
	t.Run("AddLink", func(t *testing.T) {
		t.Log("Adding link link1...")
		if err := kbManager.AddLink("kb1", "people.john", "link1"); err != nil {
			t.Fatalf("Error adding link: %v", err)
		}
		t.Log("Successfully added link link1")
	})
}


