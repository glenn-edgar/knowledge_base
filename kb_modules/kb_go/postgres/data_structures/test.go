// TestServerFunctions tests RPC server functionality

package main

import (
	"fmt"
	"log"
	"github.com/google/uuid"
	"github.com/glenn-edgar/knowledge_base/kb_modules/kb_go/postgres/data_structures/data_structures_module"
)

func (kds *KBDataStructures) TestServerFunctions(serverPath string) error {
	fmt.Printf("rpc_server_path: %s\n", serverPath)
	fmt.Println("initial state")

	fmt.Println("clear server queue")
	if err := kds.RPCServerClearServerQueue(serverPath); err != nil {
		return err
	}

	jobs, err := kds.RPCServerListJobsJobTypes(serverPath, "new_job")
	if err != nil {
		return err
	}
	fmt.Printf("list_jobs_job_types: %v\n", jobs)

	requestID1 := uuid.New().String()
	err = kds.RPCServerPushRPCQueue(serverPath, requestID1, "rpc_action1", 
		map[string]interface{}{"data1": "data1"}, "transaction_tag_1", 1, 
		"rpc_client_queue", 5, 0.5)
	if err != nil {
		return err
	}

	requestID2 := uuid.New().String()
	err = kds.RPCServerPushRPCQueue(serverPath, requestID2, "rpc_action2", 
		map[string]interface{}{"data2": "data1"}, "transaction_tag_2", 2, 
		"rpc_client_queue", 5, 0.5)
	if err != nil {
		return err
	}

	jobs, _ = kds.RPCServerListJobsJobTypes(serverPath, "new_job")
	fmt.Printf("list_jobs_job_types: %v\n", jobs)

	requestID3 := uuid.New().String()
	err = kds.RPCServerPushRPCQueue(serverPath, requestID3, "rpc_action3", 
		map[string]interface{}{"data3": "data1"}, "transaction_tag_3", 3, 
		"rpc_client_queue", 5, 0.5)
	if err != nil {
		return err
	}

	fmt.Printf("request_ids: %s, %s, %s\n", requestID1, requestID2, requestID3)

	jobData1, err := kds.RPCServerPeakServerQueue(serverPath)
	if err != nil {
		return err
	}
	fmt.Printf("job_data_1: %v\n", jobData1)

	kds.RPCServerCountAllJobs(serverPath)
	jobData2, _ := kds.RPCServerPeakServerQueue(serverPath)
	fmt.Printf("job_data_2: %v\n", jobData2)

	kds.RPCServerCountAllJobs(serverPath)
	jobData3, _ := kds.RPCServerPeakServerQueue(serverPath)
	fmt.Printf("job_data_3: %v\n", jobData3)

	if id1, ok := jobData1["id"]; ok {
		kds.RPCServerMarkJobCompletion(serverPath, id1)
	}

	if id2, ok := jobData2["id"]; ok {
		kds.RPCServerMarkJobCompletion(serverPath, id2)
	}

	if id3, ok := jobData3["id"]; ok {
		kds.RPCServerMarkJobCompletion(serverPath, id3)
	}

	return nil
}

// TestClientQueue tests RPC client functionality
func (kds *KBDataStructures) TestClientQueue(clientPath string) error {
	fmt.Println("=== Initial State ===")
	
	freeSlots, err := kds.RPCClientFindFreeSlots(clientPath)
	if err != nil {
		return err
	}
	fmt.Printf("Number of free slots: %d\n", freeSlots)

	queuedSlots, err := kds.RPCClientFindQueuedSlots(clientPath)
	if err != nil {
		return err
	}
	fmt.Printf("Number of queued slots: %d\n", queuedSlots)

	waitingJobs, err := kds.RPCClientListWaitingJobs(clientPath)
	if err != nil {
		return err
	}
	fmt.Printf("Waiting jobs: %v\n", waitingJobs)

	err = kds.RPCClientClearReplyQueue(clientPath)
	if err != nil {
		return err
	}

	fmt.Println("\n=== Pushing First Set of Reply Data ===")
	requestID1 := uuid.New()
	err = kds.RPCClientPushAndClaimReplyData(clientPath, requestID1, "xxx", "Action1", "xxx", 
		map[string]interface{}{"data1": "data1"})
	if err != nil {
		return err
	}
	fmt.Printf("Pushed reply data with request ID: %s\n", requestID1.String())

	requestID2 := uuid.New().String()
	err = kds.RPCClientPushAndClaimReplyData(clientPath, requestID2, "xxx", "Action2", "yyy", 
		map[string]interface{}{"data2": "data2"})
	if err != nil {
		return err
	}
	fmt.Printf("Pushed reply data with request ID: %s\n", requestID2)

	// Continue with the rest of the test logic...
	fmt.Println("\n=== Peek and Release First Data ===")
	peakData, err := kds.RPCClientPeakAndClaimReplyData(clientPath)
	if err != nil {
		return err
	}
	fmt.Printf("Peak data: %v\n", peakData)

	return nil
}

// Example usage and main function
func main() {
	// This would typically get the password from secure input
	password := "your_password"

	kbDataStructures, err := NewKBDataStructures(
		"localhost",        // host
		"5432",            // port
		"knowledge_base",   // dbname
		"gedgar",          // user
		password,          // password
		"knowledge_base",   // database
	)
	if err != nil {
		log.Fatal("Failed to create KB_Data_Structures:", err)
	}
	defer kbDataStructures.Disconnect()

	// Example status data test
	fmt.Println("***************************  status data ***************************")
	
	nodeIDs, err := kbDataStructures.FindStatusNodeIDs(nil, nil, nil, nil)
	if err != nil {
		log.Printf("Error finding status node IDs: %v", err)
	} else {
		fmt.Printf("node_ids: %v\n", nodeIDs)
		
		pathValues, err := kbDataStructures.FindPathValues(nodeIDs)
		if err != nil {
			log.Printf("Error finding path values: %v", err)
		} else {
			fmt.Printf("path_values: %v\n", pathValues)
		}
	}

	// Example job queue test
	fmt.Println("***************************  job queue data ***************************")
	
	jobIDs, err := kbDataStructures.FindJobIDs(nil, nil, nil, nil)
	if err != nil {
		log.Printf("Error finding job IDs: %v", err)
	} else {
		fmt.Printf("job_ids: %v\n", jobIDs)
	}

	// Example link table test
	fmt.Println("***************************  Link Tables ***************************")
	
	linkNames, err := kbDataStructures.LinkTableFindAllLinkNames()
	if err != nil {
		log.Printf("Error finding link names: %v", err)
	} else {
		fmt.Printf("find_all_link_names: %v\n", linkNames)
	}

	nodeNames, err := kbDataStructures.LinkTableFindAllNodeNames()
	if err != nil {
		log.Printf("Error finding node names: %v", err)
	} else {
		fmt.Printf("find_all_node_names: %v\n", nodeNames)
	}

	fmt.Println("Knowledge Base Data Structures initialized successfully!")
}
