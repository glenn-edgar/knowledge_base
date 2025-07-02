package main

import (
	"fmt"
	"log"
	"strings"
)

// ConstructMemDB extends BasicConstructDB with knowledge base management and composite path tracking
type ConstructMemDB struct {
	*BasicConstructDB                    // Embedded struct for inheritance-like behavior
	kbName              *string          // Currently selected knowledge base name
	workingKB           *string          // Working knowledge base
	compositePath       map[string][]string          // Tracks composite paths for each KB
	compositePathValues map[string]map[string]bool   // Tracks existing paths in each KB
}

// NewConstructMemDB creates a new ConstructMemDB instance
func NewConstructMemDB(host string, port int, dbname, user, password, database string) *ConstructMemDB {
	return &ConstructMemDB{
		BasicConstructDB:    NewBasicConstructDB(host, port, dbname, user, password, database),
		kbName:              nil,
		workingKB:           nil,
		compositePath:       make(map[string][]string),
		compositePathValues: make(map[string]map[string]bool),
	}
}

// AddKB adds a knowledge base with composite path tracking
func (cmdb *ConstructMemDB) AddKB(kbName, description string) error {
	// Check if KB already exists in composite path
	if _, exists := cmdb.compositePath[kbName]; exists {
		return fmt.Errorf("knowledge base %s already exists", kbName)
	}

	// Initialize composite path structures
	cmdb.compositePath[kbName] = []string{kbName}
	cmdb.compositePathValues[kbName] = make(map[string]bool)

	// Call parent method
	return cmdb.BasicConstructDB.AddKB(kbName, description)
}

// SelectKB selects a knowledge base to work with
func (cmdb *ConstructMemDB) SelectKB(kbName string) error {
	if _, exists := cmdb.compositePath[kbName]; !exists {
		return fmt.Errorf("knowledge base %s does not exist", kbName)
	}
	cmdb.workingKB = &kbName
	return nil
}

// AddHeaderNode adds a header node to the knowledge base
func (cmdb *ConstructMemDB) AddHeaderNode(link, nodeName string, nodeData map[string]interface{}, description string) error {
	if cmdb.workingKB == nil {
		return fmt.Errorf("no working knowledge base selected")
	}

	// Validate input types
	if nodeData == nil {
		return fmt.Errorf("nodeData must be a dictionary")
	}

	// Add description if provided
	if description != "" {
		nodeData["description"] = description
	}

	// Build composite path
	cmdb.compositePath[*cmdb.workingKB] = append(cmdb.compositePath[*cmdb.workingKB], link)
	cmdb.compositePath[*cmdb.workingKB] = append(cmdb.compositePath[*cmdb.workingKB], nodeName)
	nodePath := strings.Join(cmdb.compositePath[*cmdb.workingKB], ".")

	// Check if path already exists
	if cmdb.compositePathValues[*cmdb.workingKB][nodePath] {
		return fmt.Errorf("path %s already exists in knowledge base", nodePath)
	}

	// Mark path as used
	cmdb.compositePathValues[*cmdb.workingKB][nodePath] = true

	// Store in the underlying BasicConstructDB
	path := strings.Join(cmdb.compositePath[*cmdb.workingKB], ".")
	fmt.Println("path", path)
	return cmdb.BasicConstructDB.Store(path, nodeData, nil, nil)
}

// AddInfoNode adds an info node (temporary header node that gets removed from path)
func (cmdb *ConstructMemDB) AddInfoNode(link, nodeName string, nodeData map[string]interface{}, description string) error {
	if cmdb.workingKB == nil {
		return fmt.Errorf("no working knowledge base selected")
	}

	// Add as header node first
	err := cmdb.AddHeaderNode(link, nodeName, nodeData, description)
	if err != nil {
		return err
	}

	// Remove node_name and link from path (reverse order)
	pathLen := len(cmdb.compositePath[*cmdb.workingKB])
	if pathLen >= 2 {
		cmdb.compositePath[*cmdb.workingKB] = cmdb.compositePath[*cmdb.workingKB][:pathLen-1] // Remove nodeName
		cmdb.compositePath[*cmdb.workingKB] = cmdb.compositePath[*cmdb.workingKB][:pathLen-2] // Remove link
	}

	return nil
}

// LeaveHeaderNode leaves a header node, verifying the label and name
func (cmdb *ConstructMemDB) LeaveHeaderNode(label, name string) error {
	if cmdb.workingKB == nil {
		return fmt.Errorf("no working knowledge base selected")
	}

	path := cmdb.compositePath[*cmdb.workingKB]

	// Check if path is empty
	if len(path) == 0 {
		return fmt.Errorf("cannot leave a header node: path is empty")
	}

	// Pop the name
	if len(path) < 1 {
		return fmt.Errorf("cannot leave a header node: path is empty")
	}
	refName := path[len(path)-1]
	cmdb.compositePath[*cmdb.workingKB] = path[:len(path)-1]

	// Check if we have enough elements for label
	path = cmdb.compositePath[*cmdb.workingKB]
	if len(path) == 0 {
		// Put the name back and raise an error
		cmdb.compositePath[*cmdb.workingKB] = append(cmdb.compositePath[*cmdb.workingKB], refName)
		return fmt.Errorf("cannot leave a header node: not enough elements in path")
	}

	// Pop the label
	refLabel := path[len(path)-1]
	cmdb.compositePath[*cmdb.workingKB] = path[:len(path)-1]

	// Verify the popped values
	var errorMsgs []string
	if refName != name {
		errorMsgs = append(errorMsgs, fmt.Sprintf("expected name '%s', but got '%s'", name, refName))
	}
	if refLabel != label {
		errorMsgs = append(errorMsgs, fmt.Sprintf("expected label '%s', but got '%s'", label, refLabel))
	}

	if len(errorMsgs) > 0 {
		return fmt.Errorf("assertion error: %s", strings.Join(errorMsgs, ", "))
	}

	return nil
}

// CheckInstallation checks if the installation is correct by verifying that all paths are properly reset
func (cmdb *ConstructMemDB) CheckInstallation() error {
	for kbName, path := range cmdb.compositePath {
		if len(path) != 1 {
			return fmt.Errorf("installation check failed: path is not empty for knowledge base %s. Path: %v", kbName, path)
		}
		if path[0] != kbName {
			return fmt.Errorf("installation check failed: path is not empty for knowledge base %s. Path: %v", kbName, path)
		}
	}
	return nil
}

// GetCurrentPath returns the current composite path for the working KB
func (cmdb *ConstructMemDB) GetCurrentPath() []string {
	if cmdb.workingKB == nil {
		return nil
	}
	// Return a copy to prevent external modification
	path := make([]string, len(cmdb.compositePath[*cmdb.workingKB]))
	copy(path, cmdb.compositePath[*cmdb.workingKB])
	return path
}

// GetCurrentPathString returns the current composite path as a string
func (cmdb *ConstructMemDB) GetCurrentPathString() string {
	if cmdb.workingKB == nil {
		return ""
	}
	return strings.Join(cmdb.compositePath[*cmdb.workingKB], ".")
}

// GetWorkingKB returns the currently selected working knowledge base
func (cmdb *ConstructMemDB) GetWorkingKB() *string {
	return cmdb.workingKB
}

// GetAllKBNames returns all knowledge base names
func (cmdb *ConstructMemDB) GetAllKBNames() []string {
	names := make([]string, 0, len(cmdb.compositePath))
	for name := range cmdb.compositePath {
		names = append(names, name)
	}
	return names
}

// ExampleUsage demonstrates how to use ConstructMemDB
func ExampleConstructMemDBUsage() {
	fmt.Println("Starting unit test")

	// Replace with your actual database credentials
	dbHost := "localhost"
	dbPort := 5432
	dbName := "knowledge_base"
	dbUser := "gedgar"
	dbPassword := "password" // In real usage, get this securely
	dbTable := "knowledge_base"

	kb := NewConstructMemDB(dbHost, dbPort, dbName, dbUser, dbPassword, dbTable)

	// Test KB1
	err := kb.AddKB("kb1", "First knowledge base")
	if err != nil {
		log.Printf("Error adding kb1: %v", err)
		return
	}

	err = kb.SelectKB("kb1")
	if err != nil {
		log.Printf("Error selecting kb1: %v", err)
		return
	}

	err = kb.AddHeaderNode("header1_link", "header1_name", map[string]interface{}{"data": "header1_data"}, "header1_description")
	if err != nil {
		log.Printf("Error adding header1: %v", err)
		return
	}

	err = kb.AddInfoNode("info1_link", "info1_name", map[string]interface{}{"data": "info1_data"}, "info1_description")
	if err != nil {
		log.Printf("Error adding info1: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header1_link", "header1_name")
	if err != nil {
		log.Printf("Error leaving header1: %v", err)
		return
	}

	err = kb.AddHeaderNode("header2_link", "header2_name", map[string]interface{}{"data": "header2_data"}, "header2_description")
	if err != nil {
		log.Printf("Error adding header2: %v", err)
		return
	}

	err = kb.AddInfoNode("info2_link", "info2_name", map[string]interface{}{"data": "info2_data"}, "info2_description")
	if err != nil {
		log.Printf("Error adding info2: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header2_link", "header2_name")
	if err != nil {
		log.Printf("Error leaving header2: %v", err)
		return
	}

	// Test KB2
	err = kb.AddKB("kb2", "Second knowledge base")
	if err != nil {
		log.Printf("Error adding kb2: %v", err)
		return
	}

	err = kb.SelectKB("kb2")
	if err != nil {
		log.Printf("Error selecting kb2: %v", err)
		return
	}

	err = kb.AddHeaderNode("header1_link", "header1_name", map[string]interface{}{"data": "header1_data"}, "header1_description")
	if err != nil {
		log.Printf("Error adding header1 to kb2: %v", err)
		return
	}

	err = kb.AddInfoNode("info1_link", "info1_name", map[string]interface{}{"data": "info1_data"}, "info1_description")
	if err != nil {
		log.Printf("Error adding info1 to kb2: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header1_link", "header1_name")
	if err != nil {
		log.Printf("Error leaving header1 in kb2: %v", err)
		return
	}

	err = kb.AddHeaderNode("header2_link", "header2_name", map[string]interface{}{"data": "header2_data"}, "header2_description")
	if err != nil {
		log.Printf("Error adding header2 to kb2: %v", err)
		return
	}

	err = kb.AddInfoNode("info2_link", "info2_name", map[string]interface{}{"data": "info2_data"}, "info2_description")
	if err != nil {
		log.Printf("Error adding info2 to kb2: %v", err)
		return
	}

	err = kb.LeaveHeaderNode("header2_link", "header2_name")
	if err != nil {
		log.Printf("Error leaving header2 in kb2: %v", err)
		return
	}

	// Check installation
	err = kb.CheckInstallation()
	if err != nil {
		log.Printf("Error during installation check: %v", err)
		return
	}

	// Export and import from PostgreSQL
	exported, err := kb.ExportToPostgres("composite_memory_kb", true, true)
	if err != nil {
		log.Printf("Export error: %v", err)
	} else {
		fmt.Printf("Exported %d records\n", exported)
	}

	imported, err := kb.ImportFromPostgres("composite_memory_kb", "path", "data", "created_at", "updated_at")
	if err != nil {
		log.Printf("Import error: %v", err)
	} else {
		fmt.Printf("Imported %d records\n", imported)
	}

	fmt.Println("Ending unit test")
}

func main() {
	// Run the example
	ExampleConstructMemDBUsage()
}

