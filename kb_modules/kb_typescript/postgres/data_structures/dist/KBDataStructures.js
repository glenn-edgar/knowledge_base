"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KBDataStructures = void 0;
const KBSearch_1 = require("./KBSearch");
const KBStatusData_1 = require("./KBStatusData");
const KBJobQueue_1 = require("./KBJobQueue");
const KBStream_1 = require("./KBStream");
const KBRpcClient_1 = require("./KBRpcClient");
const KBRpcServer_1 = require("./KBRpcServer");
const KBLinkTable_1 = require("./KBLinkTable");
const KBLinkMountTable_1 = require("./KBLinkMountTable");
class KBDataStructures {
    constructor(host, port, dbName, user, password, database) {
        // Core search
        this.querySupport = new KBSearch_1.KBSearch(host, port, dbName, user, password, database);
        this.clearFilters = this.querySupport.clearFilters.bind(this.querySupport);
        this.searchLabel = this.querySupport.searchLabel.bind(this.querySupport);
        this.searchName = this.querySupport.searchName.bind(this.querySupport);
        this.searchPropertyKey = this.querySupport.searchPropertyKey.bind(this.querySupport);
        this.searchPropertyValue = this.querySupport.searchPropertyValue.bind(this.querySupport);
        this.searchHasLink = this.querySupport.searchHasLink.bind(this.querySupport);
        this.searchHasLinkMount = this.querySupport.searchHasLinkMount.bind(this.querySupport);
        this.searchPath = this.querySupport.searchPath.bind(this.querySupport);
        this.searchStartingPath = this.querySupport.searchStartingPath.bind(this.querySupport);
        this.executeKbSearch = this.querySupport.executeQuery.bind(this.querySupport);
        this.findDescription = this.querySupport.findDescription.bind(this.querySupport);
        this.findDescriptionPaths = this.querySupport.findDescriptionPaths.bind(this.querySupport);
        this.findPathValues = this.querySupport.findPathValues.bind(this.querySupport);
        this.decodeLinkNodes = this.querySupport.decodeLinkNodes.bind(this.querySupport);
        // Status Data
        this.statusData = new KBStatusData_1.KBStatusData(this.querySupport, database);
        this.findStatusNodeIds = this.statusData.findNodeIds.bind(this.statusData);
        this.findStatusNodeId = this.statusData.findNodeId.bind(this.statusData);
        this.getStatusData = this.statusData.getStatusData.bind(this.statusData);
        this.setStatusData = this.statusData.setStatusData.bind(this.statusData);
        // Job Queue
        this.jobQueue = new KBJobQueue_1.KBJobQueue(this.querySupport, database);
        this.findJobIds = this.jobQueue.findJobIds.bind(this.jobQueue);
        this.findJobId = this.jobQueue.findJobId.bind(this.jobQueue);
        this.getQueuedNumber = this.jobQueue.getQueuedNumber.bind(this.jobQueue);
        this.getFreeNumber = this.jobQueue.getFreeNumber.bind(this.jobQueue);
        this.peakJobData = this.jobQueue.peakJobData.bind(this.jobQueue);
        this.markJobCompleted = this.jobQueue.markJobCompleted.bind(this.jobQueue);
        this.pushJobData = this.jobQueue.pushJobData.bind(this.jobQueue);
        this.listPendingJobs = this.jobQueue.listPendingJobs.bind(this.jobQueue);
        this.listActiveJobs = this.jobQueue.listActiveJobs.bind(this.jobQueue);
        this.clearJobQueue = this.jobQueue.clearJobQueue.bind(this.jobQueue);
        // Stream
        this.stream = new KBStream_1.KBStream(this.querySupport, database);
        this.findStreamIds = this.stream.findStreamIds.bind(this.stream);
        this.findStreamId = this.stream.findStreamId.bind(this.stream);
        this.findStreamTableKeys = this.stream.findStreamTableKeys.bind(this.stream);
        this.pushStreamData = this.stream.pushStreamData.bind(this.stream);
        this.listStreamData = this.stream.listStreamData.bind(this.stream);
        this.clearStreamData = this.stream.clearStreamData.bind(this.stream);
        this.getStreamDataCount = this.stream.getStreamDataCount.bind(this.stream);
        this.getStreamDataRange = this.stream.getStreamDataRange.bind(this.stream);
        this.getStreamStatistics = this.stream.getStreamStatistics.bind(this.stream);
        this.getStreamDataById = this.stream.getStreamDataById.bind(this.stream);
        // RPC Client
        this.rpcClient = new KBRpcClient_1.KBRpcClient(this.querySupport, database);
        this.rpcClientFindRpcClientId = this.rpcClient.findRpcClientId.bind(this.rpcClient);
        this.rpcClientFindRpcClientIds = this.rpcClient.findRpcClientIds.bind(this.rpcClient);
        this.rpcClientFindRpcClientKeys = this.rpcClient.findRpcClientKeys.bind(this.rpcClient);
        this.rpcClientFindFreeSlots = this.rpcClient.findFreeSlots.bind(this.rpcClient);
        this.rpcClientFindQueuedSlots = this.rpcClient.findQueuedSlots.bind(this.rpcClient);
        this.rpcClientPeakAndClaimReplyData = this.rpcClient.peakAndClaimReplyData.bind(this.rpcClient);
        this.rpcClientClearReplyQueue = this.rpcClient.clearReplyQueue.bind(this.rpcClient);
        this.rpcClientPushAndClaimReplyData = this.rpcClient.pushAndClaimReplyData.bind(this.rpcClient);
        this.rpcClientListWaitingJobs = this.rpcClient.listWaitingJobs.bind(this.rpcClient);
        // RPC Server
        this.rpcServer = new KBRpcServer_1.KBRpcServer(this.querySupport, database);
        this.rpcServerFindId = this.rpcServer.findRpcServerId.bind(this.rpcServer);
        this.rpcServerFindIds = this.rpcServer.findRpcServerIds.bind(this.rpcServer);
        this.rpcServerFindTableKeys = this.rpcServer.findRpcServerTableKeys.bind(this.rpcServer);
        this.rpcServerListJobsJobTypes = this.rpcServer.listJobsJobTypes.bind(this.rpcServer);
        this.rpcServerCountAllJobs = this.rpcServer.countAllJobs.bind(this.rpcServer);
        this.rpcServerCountEmptyJobs = this.rpcServer.countEmptyJobs.bind(this.rpcServer);
        this.rpcServerCountNewJobs = this.rpcServer.countNewJobs.bind(this.rpcServer);
        this.rpcServerCountProcessingJobs = this.rpcServer.countProcessingJobs.bind(this.rpcServer);
        this.rpcServerCountJobsJobTypes = this.rpcServer.countJobsJobTypes.bind(this.rpcServer);
        this.rpcServerPushRpcQueue = this.rpcServer.pushRpcQueue.bind(this.rpcServer);
        this.rpcServerPeakServerQueue = this.rpcServer.peakServerQueue.bind(this.rpcServer);
        this.rpcServerMarkJobCompletion = this.rpcServer.markJobCompletion.bind(this.rpcServer);
        this.rpcServerClearServerQueue = this.rpcServer.clearServerQueue.bind(this.rpcServer);
        // Link Table
        this.linkTable = new KBLinkTable_1.KBLinkTable(this.querySupport.getConn(), database);
        this.linkTableFindRecordsByLinkName = this.linkTable.findRecordsByLinkName.bind(this.linkTable);
        this.linkTableFindRecordsByNodePath = this.linkTable.findRecordsByNodePath.bind(this.linkTable);
        this.linkTableFindAllLinkNames = this.linkTable.findAllLinkNames.bind(this.linkTable);
        this.linkTableFindAllNodeNames = this.linkTable.findAllNodeNames.bind(this.linkTable);
        // Link Mount Table
        this.linkMountTable = new KBLinkMountTable_1.KBLinkMountTable(this.querySupport.getConn(), database);
        this.linkMountTableFindRecordsByLinkName = this.linkMountTable.findRecordsByLinkName.bind(this.linkMountTable);
        this.linkMountTableFindRecordsByMountPath = this.linkMountTable.findRecordsByMountPath.bind(this.linkMountTable);
        this.linkMountTableFindAllLinkNames = this.linkMountTable.findAllLinkNames.bind(this.linkMountTable);
        this.linkMountTableFindAllMountPaths = this.linkMountTable.findAllMountPaths.bind(this.linkMountTable);
    }
}
exports.KBDataStructures = KBDataStructures;
