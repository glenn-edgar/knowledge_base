package main

import (
	"fmt"
	"log"
	"strings"
)

// SearchMemDB extends BasicConstructDB with search and filtering capabilities
type SearchMemDB struct {
	*BasicConstructDB                    // Embedded struct for inheritance-like behavior
	keys            map[string][]string  // Generated decoded keys
	kbs             map[string][]string  // Knowledge bases mapping
	labels          map[string][]string  // Labels mapping
	names           map[string][]string  // Names mapping
	decodedKeys     map[string][]string  // Decoded path keys
	filterResults   map[string]*TreeNode // Current filter results
}

// NewSearchMemDB creates a new SearchMemDB instance and loads data from PostgreSQL
func NewSearchMemDB(host string, port int, dbname, user, password, tableName string) (*SearchMemDB, error) {
	smdb := &SearchMemDB{
		BasicConstructDB: NewBasicConstructDB(host, port, dbname, user, password, tableName),
		kbs:              make(map[string][]string),
		labels:           make(map[string][]string),
		names:            make(map[string][]string),
		decodedKeys:      make(map[string][]string),
		filterResults:    make(map[string]*TreeNode),
	}

	// Import data from PostgreSQL
	_, err := smdb.ImportFromPostgres(tableName, "path", "data", "created_at", "updated_at")
	if err != nil {
		return nil, fmt.Errorf("failed to import from postgres: %w", err)
	}

	// Generate decoded keys
	smdb.keys = smdb.generateDecodedKeys(smdb.data)
	
	// Initialize filter results with all data
	smdb.ClearFilters()

	return smdb, nil
}

// generateDecodedKeys processes the data and creates lookup maps
func (smdb *SearchMemDB) generateDecodedKeys(data map[string]*TreeNode) map[string][]string {
	smdb.kbs = make(map[string][]string)
	smdb.labels = make(map[string][]string)
	smdb.names = make(map[string][]string)
	smdb.decodedKeys = make(map[string][]string)

	for key := range data {
		// Split the key into components
		smdb.decodedKeys[key] = strings.Split(key, ".")
		
		if len(smdb.decodedKeys[key]) < 3 {
			// Skip keys that don't have at least kb.label.name structure
			continue
		}

		kb := smdb.decodedKeys[key][0]
		label := smdb.decodedKeys[key][len(smdb.decodedKeys[key])-2]
		name := smdb.decodedKeys[key][len(smdb.decodedKeys[key])-1]

		// Add to knowledge bases map
		if _, exists := smdb.kbs[kb]; !exists {
			smdb.kbs[kb] = make([]string, 0)
		}
		smdb.kbs[kb] = append(smdb.kbs[kb], key)

		// Add to labels map
		if _, exists := smdb.labels[label]; !exists {
			smdb.labels[label] = make([]string, 0)
		}
		smdb.labels[label] = append(smdb.labels[label], key)

		// Add to names map
		if _, exists := smdb.names[name]; !exists {
			smdb.names[name] = make([]string, 0)
		}
		smdb.names[name] = append(smdb.names[name], key)
	}

	return smdb.decodedKeys
}

// ClearFilters clears all filters and resets the query state
func (smdb *SearchMemDB) ClearFilters() {
	smdb.filterResults = make(map[string]*TreeNode)
	// Copy all data to filter results
	for key, value := range smdb.data {
		smdb.filterResults[key] = value
	}
}

// SearchKB searches for rows matching the specified knowledge base
func (smdb *SearchMemDB) SearchKB(knowledgeBase string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	if kbKeys, exists := smdb.kbs[knowledgeBase]; exists {
		for _, key := range kbKeys {
			if _, exists := smdb.filterResults[key]; exists {
				newFilterResults[key] = smdb.filterResults[key]
			}
		}
	}
	
	smdb.filterResults = newFilterResults
	return smdb.filterResults
}

// SearchLabel searches for rows matching the specified label
func (smdb *SearchMemDB) SearchLabel(label string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	if labelKeys, exists := smdb.labels[label]; exists {
		for _, key := range labelKeys {
			if _, exists := smdb.filterResults[key]; exists {
				newFilterResults[key] = smdb.filterResults[key]
			}
		}
	}
	
	smdb.filterResults = newFilterResults
	return smdb.filterResults
}

// SearchName searches for rows matching the specified name
func (smdb *SearchMemDB) SearchName(name string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	if nameKeys, exists := smdb.names[name]; exists {
		for _, key := range nameKeys {
			if _, exists := smdb.filterResults[key]; exists {
				newFilterResults[key] = smdb.filterResults[key]
			}
		}
	}
	
	smdb.filterResults = newFilterResults
	return smdb.filterResults
}

// SearchPropertyKey searches for rows that contain the specified property key
func (smdb *SearchMemDB) SearchPropertyKey(dataKey string) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	for key := range smdb.filterResults {
		if node, exists := smdb.data[key]; exists {
			if dataMap, ok := node.Data.(map[string]interface{}); ok {
				if _, hasKey := dataMap[dataKey]; hasKey {
					newFilterResults[key] = smdb.filterResults[key]
				}
			}
		}
	}
	
	smdb.filterResults = newFilterResults
	return smdb.filterResults
}

// SearchPropertyValue searches for rows where the properties JSON field contains the specified key with the specified value
func (smdb *SearchMemDB) SearchPropertyValue(dataKey string, dataValue interface{}) map[string]*TreeNode {
	newFilterResults := make(map[string]*TreeNode)
	
	for key := range smdb.filterResults {
		if node, exists := smdb.data[key]; exists {
			if dataMap, ok := node.Data.(map[string]interface{}); ok {
				if value, hasKey := dataMap[dataKey]; hasKey {
					if value == dataValue {
						newFilterResults[key] = smdb.filterResults[key]
					}
				}
			}
		}
	}
	
	smdb.filterResults = newFilterResults
	return smdb.filterResults
}

// SearchStartingPath searches for a specific path and all its descendants
func (smdb *SearchMemDB) SearchStartingPath(startingPath string) (map[string]*TreeNode, error) {
	newFilterResults := make(map[string]*TreeNode)
	
	// Add starting path if it exists in filter results
	if _, exists := smdb.filterResults[startingPath]; exists {
		newFilterResults[startingPath] = smdb.filterResults[startingPath]
	} else {
		// If starting path doesn't exist, clear filter results
		smdb.filterResults = make(map[string]*TreeNode)
		return newFilterResults, nil
	}
	
	// Get and add descendants
	descendants, err := smdb.QueryDescendants(startingPath)
	if err != nil {
		return nil, fmt.Errorf("error querying descendants: %w", err)
	}
	
	for _, item := range descendants {
		if _, exists := smdb.filterResults[item.Path]; exists {
			newFilterResults[item.Path] = smdb.filterResults[item.Path]
		}
	}
	
	smdb.filterResults = newFilterResults
	return newFilterResults, nil
}

// SearchPath searches for rows matching the specified LTREE path expression using operators
func (smdb *SearchMemDB) SearchPath(operator, startingPath string) map[string]*TreeNode {
	// Use the parent class query method
	searchResults := smdb.QueryByOperator(operator, startingPath, "")
	
	newFilterResults := make(map[string]*TreeNode)
	for _, item := range searchResults {
		if _, exists := smdb.filterResults[item.Path]; exists {
			newFilterResults[item.Path] = smdb.filterResults[item.Path]
		}
	}
	
	smdb.filterResults = newFilterResults
	return smdb.filterResults
}

// FindDescriptions extracts descriptions from all data entries or a specific key
func (smdb *SearchMemDB) FindDescriptions(key interface{}) map[string]string {
	returnValues := make(map[string]string)
	
	// Process all data entries
	for rowKey, rowData := range smdb.data {
		if dataMap, ok := rowData.Data.(map[string]interface{}); ok {
			if description, exists := dataMap["description"]; exists {
				if descStr, ok := description.(string); ok {
					returnValues[rowKey] = descStr
				} else {
					returnValues[rowKey] = ""
				}
			} else {
				returnValues[rowKey] = ""
			}
		} else {
			returnValues[rowKey] = ""
		}
	}
	
	return returnValues
}

// GetFilterResults returns the current filter results
func (smdb *SearchMemDB) GetFilterResults() map[string]*TreeNode {
	// Return a copy to prevent external modification
	results := make(map[string]*TreeNode)
	for key, value := range smdb.filterResults {
		results[key] = value
	}
	return results
}

// GetFilterResultKeys returns just the keys of current filter results
func (smdb *SearchMemDB) GetFilterResultKeys() []string {
	keys := make([]string, 0, len(smdb.filterResults))
	for key := range smdb.filterResults {
		keys = append(keys, key)
	}
	return keys
}

// GetKBs returns all knowledge bases
func (smdb *SearchMemDB) GetKBs() map[string][]string {
	return smdb.kbs
}

// GetLabels returns all labels
func (smdb *SearchMemDB) GetLabels() map[string][]string {
	return smdb.labels
}

// GetNames returns all names
func (smdb *SearchMemDB) GetNames() map[string][]string {
	return smdb.names
}

// GetDecodedKeys returns all decoded keys
func (smdb *SearchMemDB) GetDecodedKeys() map[string][]string {
	return smdb.decodedKeys
}

// ExampleSearchMemDBUsage demonstrates how to use SearchMemDB
func ExampleSearchMemDBUsage() {
	fmt.Println("Starting SearchMemDB example")

	// Replace with your actual database credentials
	dbHost := "localhost"
	dbPort := 5432
	dbName := "knowledge_base"
	dbUser := "gedgar"
	dbPassword := "password" // In real usage, get this securely
	tableName := "composite_memory_kb"

	kb, err := NewSearchMemDB(dbHost, dbPort, dbName, dbUser, dbPassword, tableName)
	if err != nil {
		log.Printf("Error creating SearchMemDB: %v", err)
		return
	}

	fmt.Printf("Decoded keys: %v\n", getMapKeys(kb.decodedKeys))
	
	// Test various search operations
	fmt.Println("----------------------------------")
	
	// Search by knowledge base
	kb.ClearFilters()
	kb.SearchKB("kb1")
	fmt.Printf("Search KB results: %v\n", getMapKeys(kb.filterResults))
	
	fmt.Println("----------------------------------")
	
	// Search by label
	kb.SearchLabel("info1_link")
	fmt.Printf("Search label results: %v\n", getMapKeys(kb.filterResults))
	
	// Search by name
	kb.SearchName("info1_name")
	fmt.Printf("Search name results: %v\n", getMapKeys(kb.filterResults))
	
	fmt.Println("----------------------------------")
	
	// Search by property value
	kb.ClearFilters()
	results := kb.SearchPropertyValue("data", "info1_data")
	fmt.Printf("Search property value results: %v\n", getMapKeys(results))
	
	fmt.Println("----------------------------------")
	
	// Search by property key
	kb.ClearFilters()
	results = kb.SearchPropertyKey("data")
	fmt.Printf("Search property key results: %v\n", getMapKeys(results))
	
	fmt.Println("----------------------------------")
	
	// Search starting path
	kb.ClearFilters()
	results, err = kb.SearchStartingPath("kb2.header2_link.header2_name")
	if err != nil {
		log.Printf("Error in SearchStartingPath: %v", err)
	} else {
		fmt.Printf("Search starting path results: %v\n", getMapKeys(results))
	}
	
	fmt.Println("----------------------------------")
	
	// Search path with operator
	kb.ClearFilters()
	results = kb.SearchPath("~", "kb2.**")
	fmt.Printf("Search path results: %v\n", getMapKeys(results))
	
	fmt.Println("----------------------------------")
	
	// Find descriptions
	kb.ClearFilters()
	descriptions := kb.FindDescriptions("kb2.header2_link.header2_name")
	fmt.Printf("Find descriptions results: %v\n", descriptions)
	
	fmt.Println("----------------------------------")
}

// Helper function to extract keys from a map
func getMapKeys(m map[string]*TreeNode) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// Helper function to extract keys from string map
func getStringMapKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func main() {
	// Run the example
	ExampleSearchMemDBUsage()
}

