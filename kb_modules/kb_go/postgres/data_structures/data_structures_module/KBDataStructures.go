package data_structures_module

import (
	//database/sql"
	"fmt"
	//"log"
	//"time"

	//"github.com/google/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// KBDataStructures handles the data structures for the knowledge base
type KBDataStructures struct {
	// Core components
	querySupport    *KBSearch
	statusData      *KBStatusData
	jobQueue        *KBJobQueue
	stream          *KBStream
	rpcClient       *KBRPCClient
	rpcServer       *KBRPCServer
	linkTable       *KBLinkTable
	linkMountTable  *KBLinkMountTable
}

// NewKBDataStructures creates a new instance of KBDataStructures
func NewKBDataStructures(host, port, dbname, user, password, database string) (*KBDataStructures, error) {
	// Initialize the query support (equivalent to KB_Search)
	querySupport, err := NewKBSearch(host, port, dbname, user, password, database)
	if err != nil {
		return nil, fmt.Errorf("failed to create KB_Search: %w", err)
	}

	// Initialize all components
	statusData := NewKBStatusData(querySupport, database)
	jobQueue := NewKBJobQueue(querySupport, database)
	stream := NewKBStream(querySupport, database)
	rpcClient := NewKBRPCClient(querySupport, database)
	rpcServer := NewKBRPCServer(querySupport, database)
	linkTable := NewKBLinkTable(querySupport.conn, database)
	linkMountTable := NewKBLinkMountTable(querySupport.conn, database)

	return &KBDataStructures{
		querySupport:   querySupport,
		statusData:     statusData,
		jobQueue:       jobQueue,
		stream:         stream,
		rpcClient:      rpcClient,
		rpcServer:      rpcServer,
		linkTable:      linkTable,
		linkMountTable: linkMountTable,
	}, nil
}
/*
// Query Support Methods (delegated to querySupport)
func (kds *KBDataStructures) ClearFilters() {
	kds.querySupport.ClearFilters()
}

func (kds *KBDataStructures) SearchLabel(label string) {
	kds.querySupport.SearchLabel(label)
}

func (kds *KBDataStructures) SearchName(name string) {
	kds.querySupport.SearchName(name)
}

func (kds *KBDataStructures) SearchPropertyKey(key string) {
	kds.querySupport.SearchPropertyKey(key)
}

func (kds *KBDataStructures) SearchPropertyValue(value string,property_value map[string]interface{}) {
	kds.querySupport.SearchPropertyValue(value,property_value)
}

func (kds *KBDataStructures) SearchHasLink() {
	kds.querySupport.SearchHasLink()
}

func (kds *KBDataStructures) SearchHasLinkMount() {
	kds.querySupport.SearchHasLinkMount()
}


func (kds *KBDataStructures) SearchPath(path string) {
	kds.querySupport.SearchPath(path)
}

func (kds *KBDataStructures) SearchStartingPath(path string) {
	kds.querySupport.SearchStartingPath(path)
}

func (kds *KBDataStructures) ExecuteKBSearch(property_value map[string]interface{}) ([]map[string]interface{}, error) {
	return kds.querySupport.ExecuteQuery()
}

func (kds *KBDataStructures) FindDescription(row map[string]interface{}) map[string]string {
	return kds.querySupport.FindDescription(row)
}
func (kds *KBDataStructures) FindDescriptions(row []map[string]interface{}) []map[string]string {
	return kds.querySupport.FindDescriptions(row)
}


func (kds *KBDataStructures) FindDescriptionPaths(paths []string) ([]map[string]interface{}, error) {
	return kds.querySupport.FindDescriptionPaths(paths)
}

func (kds *KBDataStructures) FindDescriptionPath(path string) (map[string]interface{}, error) {
	return kds.querySupport.FindDescriptionPath(path)
}

func (kds *KBDataStructures) FindPathValues(keyData []map[string]interface{}) ([]string) {
	return kds.querySupport.FindPathValues(keyData)
}

func (kds *KBDataStructures) DecodeLinkNodes(path string) (string, [][]string, error) {
	return kds.querySupport.DecodeLinkNodes(path)
}








// Status Data Methods (delegated to statusData)
func (kds *KBDataStructures) FindStatusNodeIDs(kb, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	return kds.statusData.FindNodeIDs(kb, nodeName, properties, nodePath)
}



func (kds *KBDataStructures) GetStatusData(path string) (map[string]interface{},string, error) {
	return kds.statusData.GetStatusData(path)
}

func (kds *KBDataStructures) SetStatusData(path string, data map[string]interface{},retryCount int, retryDelay time.Duration) (bool ,string,error){
	return kds.statusData.SetStatusData(path, data,retryCount, retryDelay)
}

// Job Queue Methods (delegated to jobQueue)
func (kds *KBDataStructures) FindJobID(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	return kds.jobQueue.FindJobID(kb, nodeName, properties, nodePath)
}
func (kds *KBDataStructures) FindJobIDs(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	return kds.jobQueue.FindJobIDs(kb, nodeName, properties, nodePath)
}


func (kds *KBDataStructures) GetQueuedNumber(jobPath string) (int, error) {
	return kds.jobQueue.GetQueuedNumber(jobPath)
}

func (kds *KBDataStructures) GetFreeNumber(jobPath string) (int, error) {
	return kds.jobQueue.GetFreeNumber(jobPath)
}

func (kds *KBDataStructures) PeakJobData(jobPath string, maxRetries int, retryDelay time.Duration) (*PeakJobResult,error) {
	return kds.jobQueue.PeakJobData(jobPath, maxRetries, retryDelay)
}

func (kds *KBDataStructures) MarkJobCompleted(jobID int, maxRetries int, retryDelay time.Duration) (*JobCompletionResult, error) {
	return kds.jobQueue.MarkJobCompleted(jobID, maxRetries, retryDelay)
}

func (kds *KBDataStructures) PushJobData(jobPath string, data map[string]interface{}, maxRetries int, retryDelay time.Duration) (*PushJobResult, error) {
	return kds.jobQueue.PushJobData(jobPath, data, maxRetries, retryDelay)
}

func (kds *KBDataStructures) ListPendingJobs(jobPath string, limit *int, offset int) ([]JobRecord, error) {
	return kds.jobQueue.ListPendingJobs(jobPath, limit, offset)
}

func (kds *KBDataStructures) ListActiveJobs(jobPath string, limit *int, offset int) ([]JobRecord, error) {
	return kds.jobQueue.ListActiveJobs(jobPath, limit, offset)
}

func (kds *KBDataStructures) ClearJobQueue(jobPath string) (*ClearQueueResult, error) {
	return kds.jobQueue.ClearJobQueue(jobPath)
}
func (kds *KBDataStructures) FindStreamIDs(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) ([]map[string]interface{}, error) {
	return kds.stream.FindStreamIDs(kb, nodeName, properties, nodePath)
}
// Stream Methods (delegated to stream)


func (kds *KBDataStructures) FindStreamID(kb *string, nodeName *string, properties map[string]interface{}, nodePath *string) (map[string]interface{}, error) {
	return kds.stream.FindStreamID(kb, nodeName, properties, nodePath)
}

func (kds *KBDataStructures) FindStreamTableKeys(nodeIDs []map[string]interface{}) ([]string) {
	return kds.stream.FindStreamTableKeys(nodeIDs)
}

func (kds *KBDataStructures) PushStreamData(streamKey string, data map[string]interface{}, maxRetries int, retryDelay time.Duration) (*StreamPushResult, error) {
	return kds.stream.PushStreamData(streamKey, data, maxRetries, retryDelay)
}

func (kds *KBDataStructures) ListStreamData(path string, limit *int, offset int, recordedAfter, recordedBefore *time.Time, order string) ([]StreamRecord, error) {
	return kds.stream.ListStreamData(path, limit, offset, recordedAfter, recordedBefore, order)
}

func (kds *KBDataStructures) ClearStreamData(streamKey string) error {
	return kds.stream.ClearStreamData(streamKey)
}

func (kds *KBDataStructures) GetStreamDataCount(streamKey string) (int, error) {
	return kds.stream.GetStreamDataCount(streamKey)
}

func (kds *KBDataStructures) GetStreamDataRange(streamKey string, start, end int) ([]map[string]interface{}, error) {
	return kds.stream.GetStreamDataRange(streamKey, start, end)
}

func (kds *KBDataStructures) GetStreamStatistics(streamKey string) (map[string]interface{}, error) {
	return kds.stream.GetStreamStatistics(streamKey)
}

func (kds *KBDataStructures) GetStreamDataByID(streamKey string, id int) (map[string]interface{}, error) {
	return kds.stream.GetStreamDataByID(streamKey, id)
}

// RPC Client Methods (delegated to rpcClient)
func (kds *KBDataStructures) RPCClientFindRPCClientID(nodeName *string, properties map[string]interface{}, nodePath *string) ([]string, error) {
	return kds.rpcClient.FindRPCClientID(nodeName, properties, nodePath)
}

func (kds *KBDataStructures) RPCClientFindRPCClientIDs(nodeName *string, properties map[string]interface{}, nodePath *string) ([]string, error) {
	return kds.rpcClient.FindRPCClientIDs(nodeName, properties, nodePath)
}

func (kds *KBDataStructures) RPCClientFindRPCClientKeys(nodeIDs []string) ([]string, error) {
	return kds.rpcClient.FindRPCClientKeys(nodeIDs)
}

func (kds *KBDataStructures) RPCClientFindFreeSlots(clientPath string) (int, error) {
	return kds.rpcClient.FindFreeSlots(clientPath)
}

func (kds *KBDataStructures) RPCClientFindQueuedSlots(clientPath string) (int, error) {
	return kds.rpcClient.FindQueuedSlots(clientPath)
}

func (kds *KBDataStructures) RPCClientPeakAndClaimReplyData(clientPath string) (map[string]interface{}, error) {
	return kds.rpcClient.PeakAndClaimReplyData(clientPath)
}

func (kds *KBDataStructures) RPCClientClearReplyQueue(clientPath string) error {
	return kds.rpcClient.ClearReplyQueue(clientPath)
}

func (kds *KBDataStructures) RPCClientPushAndClaimReplyData(clientPath string, requestID interface{}, serverPath, action, transactionTag string, replyPayload map[string]interface{}) error {
	return kds.rpcClient.PushAndClaimReplyData(clientPath, requestID, serverPath, action, transactionTag, replyPayload)
}

func (kds *KBDataStructures) RPCClientListWaitingJobs(clientPath string) ([]map[string]interface{}, error) {
	return kds.rpcClient.ListWaitingJobs(clientPath)
}

// RPC Server Methods (delegated to rpcServer)
func (kds *KBDataStructures) RPCServerIDFind(nodeName *string, properties map[string]interface{}, nodePath *string) ([]string, error) {
	return kds.rpcServer.FindRPCServerID(nodeName, properties, nodePath)
}

func (kds *KBDataStructures) RPCServerIDsFind(nodeName *string, properties map[string]interface{}, nodePath *string) ([]string, error) {
	return kds.rpcServer.FindRPCServerIDs(nodeName, properties, nodePath)
}

func (kds *KBDataStructures) RPCServerTableKeysFind(nodeIDs []string) ([]string, error) {
	return kds.rpcServer.FindRPCServerTableKeys(nodeIDs)
}

func (kds *KBDataStructures) RPCServerListJobsJobTypes(serverPath, jobType string) ([]map[string]interface{}, error) {
	return kds.rpcServer.ListJobsJobTypes(serverPath, jobType)
}

func (kds *KBDataStructures) RPCServerCountAllJobs(serverPath string) (int, error) {
	return kds.rpcServer.CountAllJobs(serverPath)
}

func (kds *KBDataStructures) RPCServerCountEmptyJobs(serverPath string) (int, error) {
	return kds.rpcServer.CountEmptyJobs(serverPath)
}

func (kds *KBDataStructures) RPCServerCountNewJobs(serverPath string) (int, error) {
	return kds.rpcServer.CountNewJobs(serverPath)
}

func (kds *KBDataStructures) RPCServerCountProcessingJobs(serverPath string) (int, error) {
	return kds.rpcServer.CountProcessingJobs(serverPath)
}

func (kds *KBDataStructures) RPCServerCountJobsJobTypes(serverPath, jobType string) (int, error) {
	return kds.rpcServer.CountJobsJobTypes(serverPath, jobType)
}

func (kds *KBDataStructures) RPCServerPushRPCQueue(serverPath, requestID, rpcAction string, requestPayload map[string]interface{}, transactionTag string, priority int, rpcClientQueue string, maxRetries int, waitTime float64) error {
	return kds.rpcServer.PushRPCQueue(serverPath, requestID, rpcAction, requestPayload, transactionTag, priority, rpcClientQueue, maxRetries, waitTime)
}

func (kds *KBDataStructures) RPCServerPeakServerQueue(serverPath string) (map[string]interface{}, error) {
	return kds.rpcServer.PeakServerQueue(serverPath)
}

func (kds *KBDataStructures) RPCServerMarkJobCompletion(serverPath string, jobID interface{}) error {
	return kds.rpcServer.MarkJobCompletion(serverPath, jobID)
}

func (kds *KBDataStructures) RPCServerClearServerQueue(serverPath string) error {
	return kds.rpcServer.ClearServerQueue(serverPath)
}

// Link Table Methods (delegated to linkTable)
func (kds *KBDataStructures) LinkTableFindRecordsByLinkName(linkName string, kb *string) ([]map[string]interface{}, error) {
	return kds.linkTable.FindRecordsByLinkName(linkName, kb)
}

func (kds *KBDataStructures) LinkTableFindRecordsByNodePath(nodePath string, kb *string) ([]map[string]interface{}, error) {
	return kds.linkTable.FindRecordsByNodePath(nodePath, kb)
}

func (kds *KBDataStructures) LinkTableFindAllLinkNames() ([]string, error) {
	return kds.linkTable.FindAllLinkNames()
}

func (kds *KBDataStructures) LinkTableFindAllNodeNames() ([]string, error) {
	return kds.linkTable.FindAllNodeNames()
}

// Link Mount Table Methods (delegated to linkMountTable)
func (kds *KBDataStructures) LinkMountTableFindRecordsByLinkName(linkName string, kb *string) ([]map[string]interface{}, error) {
	return kds.linkMountTable.FindRecordsByLinkName(linkName, kb)
}

func (kds *KBDataStructures) LinkMountTableFindRecordsByMountPath(mountPath string, kb *string) ([]map[string]interface{}, error) {
	return kds.linkMountTable.FindRecordsByMountPath(mountPath, kb)
}

func (kds *KBDataStructures) LinkMountTableFindAllLinkNames() ([]string, error) {
	return kds.linkMountTable.FindAllLinkNames()
}

func (kds *KBDataStructures) LinkMountTableFindAllMountPaths() ([]string, error) {
	return kds.linkMountTable.FindAllMountPaths()
}

// Disconnect closes the database connection
func (kds *KBDataStructures) Disconnect() error {
	return kds.querySupport.Disconnect()
}
	*/
