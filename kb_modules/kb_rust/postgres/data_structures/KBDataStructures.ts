import { KBSearch } from './KBSearch';
import { KBStatusData } from './KBStatusData';
import { KBJobQueue } from './KBJobQueue';
import { KBStream } from './KBStream';
import { KBRpcClient } from './KBRpcClient';
import { KBRpcServer } from './KBRpcServer';
import { KBLinkTable } from './KBLinkTable';
import { KBLinkMountTable } from './KBLinkMountTable';

export class KBDataStructures {
  public querySupport: KBSearch;

  // Delegated KB_Search methods
  public clearFilters: () => void;
  public searchLabel: (label: string) => void;
  public searchName: (name: string) => void;
  public searchPropertyKey: (key: string) => void;
  public searchPropertyValue: (key: string, value: any) => void;
  public searchHasLink: () => void;
  public searchHasLinkMount: () => void;
  public searchPath: (expr: string) => void;
  public searchStartingPath: (path: string) => void;
  public executeKbSearch: () => Promise<any[]>;
  public findDescription: (data: any) => any[];
  public findDescriptionPaths: (paths: string | string[]) => Promise<Record<string, any>>;
  public findPathValues: (rows: any[] | any) => string[];
  public decodeLinkNodes: (path: string) => [string, [string, string][]];

  // Status data
  public statusData: KBStatusData;
  public findStatusNodeIds: (...args: any[]) => Promise<any[]>;
  public findStatusNodeId: (...args: any[]) => Promise<any>;
  public getStatusData: (path: string) => Promise<[any, string]>;
  public setStatusData: (path: string, data: Record<string, any>) => Promise<[boolean, string]>;

  // Job queue
  public jobQueue: KBJobQueue;
  public findJobIds: (...args: any[]) => Promise<any[]>;
  public findJobId: (...args: any[]) => Promise<any>;
  public getQueuedNumber: (path: string) => Promise<number>;
  public getFreeNumber: (path: string) => Promise<number>;
  public peakJobData: (path: string, maxRetries?: number, retryDelay?: number) => Promise<any>;
  public markJobCompleted: (jobId: number, maxRetries?: number, retryDelay?: number) => Promise<any>;
  public pushJobData: (path: string, data: Record<string, any>, maxRetries?: number, retryDelay?: number) => Promise<any>;
  public listPendingJobs: (path: string, limit?: number, offset?: number) => Promise<any[]>;
  public listActiveJobs: (path: string, limit?: number, offset?: number) => Promise<any[]>;
  public clearJobQueue: (path: string) => Promise<any>;

  // Stream
  public stream: KBStream;
  public findStreamIds: (...args: any[]) => Promise<any[]>;
  public findStreamId: (...args: any[]) => Promise<any>;
  public findStreamTableKeys: (rows: any[]) => string[];
  public pushStreamData: (path: string, data: Record<string, any>, maxRetries?: number, retryDelay?: number) => Promise<any>;
  public listStreamData: (...args: any[]) => Promise<any[]>;
  public clearStreamData: (path: string, olderThan?: Date) => Promise<any>;
  public getStreamDataCount: (path: string, includeInvalid?: boolean) => Promise<number>;
  public getStreamDataRange: (path: string, start: Date, end: Date) => Promise<any[]>;
  public getStreamStatistics: (path: string, includeInvalid?: boolean) => Promise<Record<string, any>>;
  public getStreamDataById: (id: number) => Promise<any>;

  // RPC client
  public rpcClient: KBRpcClient;
  public rpcClientFindRpcClientId: (...args: any[]) => Promise<any>;
  public rpcClientFindRpcClientIds: (...args: any[]) => Promise<any[]>;
  public rpcClientFindRpcClientKeys: (data: any[]) => string[];
  public rpcClientFindFreeSlots: (clientPath: string) => Promise<number>;
  public rpcClientFindQueuedSlots: (clientPath: string) => Promise<number>;
  public rpcClientPeakAndClaimReplyData: (clientPath: string, maxRetries?: number, retryDelay?: number) => Promise<any>;
  public rpcClientClearReplyQueue: (clientPath: string, maxRetries?: number, retryDelay?: number) => Promise<number>;
  public rpcClientPushAndClaimReplyData: (...args: any[]) => Promise<void>;
  public rpcClientListWaitingJobs: (clientPath?: string) => Promise<any[]>;

  // RPC server
  public rpcServer: KBRpcServer;
  public rpcServerFindId: (...args: any[]) => Promise<any>;
  public rpcServerFindIds: (...args: any[]) => Promise<any[]>;
  public rpcServerFindTableKeys: (data: any[]) => string[];
  public rpcServerListJobsJobTypes: (serverPath: string, state: string) => Promise<any[]>;
  public rpcServerCountAllJobs: (serverPath: string) => Promise<any>;
  public rpcServerCountEmptyJobs: (serverPath: string) => Promise<number>;
  public rpcServerCountNewJobs: (serverPath: string) => Promise<number>;
  public rpcServerCountProcessingJobs: (serverPath: string) => Promise<number>;
  public rpcServerCountJobsJobTypes: (serverPath: string, state: string) => Promise<number>;
  public rpcServerPushRpcQueue: (...args: any[]) => Promise<any>;
  public rpcServerPeakServerQueue: (serverPath: string, retries?: number, waitTime?: number) => Promise<any>;
  public rpcServerMarkJobCompletion: (...args: any[]) => Promise<any>;
  public rpcServerClearServerQueue: (serverPath: string, maxRetries?: number, retryDelay?: number) => Promise<number>;

  // Link tables
  public linkTable: KBLinkTable;
  public linkTableFindRecordsByLinkName: (linkName: string, kb?: string) => Promise<any[]>;
  public linkTableFindRecordsByNodePath: (nodePath: string, kb?: string) => Promise<any[]>;
  public linkTableFindAllLinkNames: () => Promise<string[]>;
  public linkTableFindAllNodeNames: () => Promise<string[]>;

  public linkMountTable: KBLinkMountTable;
  public linkMountTableFindRecordsByLinkName: (linkName: string, kb?: string) => Promise<any[]>;
  public linkMountTableFindRecordsByMountPath: (mountPath: string, kb?: string) => Promise<any[]>;
  public linkMountTableFindAllLinkNames: () => Promise<string[]>;
  public linkMountTableFindAllMountPaths: () => Promise<string[]>;

  constructor(
    host: string,
    port: number,
    dbName: string,
    user: string,
    password: string,
    database: string
  ) {
    // Core search
    this.querySupport = new KBSearch(host, port, dbName, user, password, database);
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
    this.statusData = new KBStatusData(this.querySupport, database);
    this.findStatusNodeIds = this.statusData.findNodeIds.bind(this.statusData);
    this.findStatusNodeId = this.statusData.findNodeId.bind(this.statusData);
    this.getStatusData = this.statusData.getStatusData.bind(this.statusData);
    this.setStatusData = this.statusData.setStatusData.bind(this.statusData);

    // Job Queue
    this.jobQueue = new KBJobQueue(this.querySupport, database);
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
    this.stream = new KBStream(this.querySupport, database);
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
    this.rpcClient = new KBRpcClient(this.querySupport, database);
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
    this.rpcServer = new KBRpcServer(this.querySupport, database);
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
    this.linkTable = new KBLinkTable(this.querySupport.getConn(), database);
    this.linkTableFindRecordsByLinkName = this.linkTable.findRecordsByLinkName.bind(this.linkTable);
    this.linkTableFindRecordsByNodePath = this.linkTable.findRecordsByNodePath.bind(this.linkTable);
    this.linkTableFindAllLinkNames = this.linkTable.findAllLinkNames.bind(this.linkTable);
    this.linkTableFindAllNodeNames = this.linkTable.findAllNodeNames.bind(this.linkTable);

    // Link Mount Table
    this.linkMountTable = new KBLinkMountTable(this.querySupport.getConn(), database);
    this.linkMountTableFindRecordsByLinkName = this.linkMountTable.findRecordsByLinkName.bind(this.linkMountTable);
    this.linkMountTableFindRecordsByMountPath = this.linkMountTable.findRecordsByMountPath.bind(this.linkMountTable);
    this.linkMountTableFindAllLinkNames = this.linkMountTable.findAllLinkNames.bind(this.linkMountTable);
    this.linkMountTableFindAllMountPaths = this.linkMountTable.findAllMountPaths.bind(this.linkMountTable);
  }
}
