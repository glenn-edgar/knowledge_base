import promptSync from 'prompt-sync';
import { KBDataStructures } from './KBDataStructures';
import { v4 as uuidv4 } from 'uuid';

const prompt = promptSync();

async function testServerFunctions(kb: KBDataStructures, serverPath: string) {
  console.log('rpc_server_path', serverPath);
  console.log('initial state');

  await kb.rpcServerClearServerQueue(serverPath);
  console.log('list_jobs_job_types', await kb.rpcServerListJobsJobTypes(serverPath, 'new_job'));

  const requestIds = [uuidv4(), uuidv4(), uuidv4()];
  for (let i = 0; i < requestIds.length; i++) {
    await kb.rpcServerPushRpcQueue(
      serverPath,
      requestIds[i],
      `rpc_action${i + 1}`,
      { [`data${i + 1}`]: `data${i + 1}` },
      `transaction_tag_${i + 1}`,
      i + 1,
      'rpc_client_queue',
      5,
      500
    );
  }
  console.log('queued after pushes', await kb.rpcServerListJobsJobTypes(serverPath, 'new_job'));

  const jobs = [
    await kb.rpcServerPeakServerQueue(serverPath),
    await kb.rpcServerPeakServerQueue(serverPath),
    await kb.rpcServerPeakServerQueue(serverPath)
  ];
  jobs.forEach((job, idx) => console.log(`job_data_${idx + 1}`, job));

  for (const job of jobs) {
    if (job?.id) {
      await kb.rpcServerMarkJobCompletion(serverPath, job.id);
      console.log('count_all_jobs', await kb.rpcServerCountAllJobs(serverPath));
    }
  }
}

async function testClientQueue(kb: KBDataStructures, clientPath: string) {
  console.log('=== Initial State ===');
  console.log('free_slots', await kb.rpcClientFindFreeSlots(clientPath));
  console.log('queued_slots', await kb.rpcClientFindQueuedSlots(clientPath));
  console.log('waiting_jobs', await kb.rpcClientListWaitingJobs(clientPath));

  await kb.rpcClientClearReplyQueue(clientPath);
  console.log('cleared, free_slots', await kb.rpcClientFindFreeSlots(clientPath));

  console.log('\n=== Pushing First Set of Reply Data ===');
  for (const action of ['Action1', 'Action2']) {
    const rid = uuidv4();
    await kb.rpcClientPushAndClaimReplyData(clientPath, rid, 'xxx', action, 'tag', { payload: action });
    console.log(`Pushed reply data ID: ${rid}`);
  }

  console.log('waiting_jobs', await kb.rpcClientListWaitingJobs(clientPath));

  console.log('\n=== Peek and Release ===');
  console.log('peeked', await kb.rpcClientPeakAndClaimReplyData(clientPath));

  await kb.rpcClientClearReplyQueue(clientPath);
  console.log('final free_slots', await kb.rpcClientFindFreeSlots(clientPath));
}

async function main() {
  const password = prompt('Enter PostgreSQL password: ');
  const kb = new KBDataStructures(
    'localhost',
    5432,
    'knowledge_base',
    'gedgar',
    password,
    'knowledge_base'
  );

  // === STATUS DATA TEST ===
  console.log('\n\n\n***************************  status data ***************************');
  const statusNodeIds = await kb.findStatusNodeIds(undefined, undefined, undefined, undefined);
  console.log('node_ids', statusNodeIds);
  const statusPaths = kb.findPathValues(statusNodeIds);
  console.log('path_values', statusPaths);

  console.log('\n=== specific status node ===');
  const specificNode = await kb.findStatusNodeId(
    'kb1', 'info2_status', { prop3: 'val3' },
    '*.header1_link.header1_name.KB_STATUS_FIELD.info2_status'
  );
  console.log('node_id', specificNode);
  const spPath = kb.findPathValues(specificNode)[0];
  console.log('path', spPath);
  console.log('description', kb.findDescription(specificNode));
  let data = await kb.getStatusData(spPath);
  console.log('initial data', data);
  await kb.setStatusData(spPath, { prop1: 'val1', prop2: 'val2' });
  data = await kb.getStatusData(spPath);
  console.log('after set', data);

  // === JOB QUEUE TEST ===
  console.log('***************************  job queue data ***************************');
  const jobNodeIds = await kb.findJobIds(undefined, undefined, undefined, undefined);
  const jobPaths = kb.findPathValues(jobNodeIds);
  const jobPath = jobPaths[0];
  console.log('first job path', jobPath);
  await kb.clearJobQueue(jobPath);
  console.log('queued_number', await kb.getQueuedNumber(jobPath));
  console.log('free_number', await kb.getFreeNumber(jobPath));
  console.log('peak empty', await kb.peakJobData(jobPath));
  await kb.pushJobData(jobPath, { prop1: 'val1', prop2: 'val2' });
  console.log('queued', await kb.getQueuedNumber(jobPath));
  console.log('free', await kb.getFreeNumber(jobPath));
  console.log('pending', await kb.listPendingJobs(jobPath));
  console.log('active', await kb.listActiveJobs(jobPath));
  const job = await kb.peakJobData(jobPath)!;
  console.log('job_id', job.id, 'data', job.data);
  await kb.markJobCompleted(job.id);
  console.log('post-complete free', await kb.getFreeNumber(jobPath));

  // === STREAM TABLES TEST ===
  console.log('***************************  stream data ***************************');
  const streamNodeIds = await kb.findStreamIds('kb1', 'info1_stream', undefined, undefined);
  const streamKeys = kb.findStreamTableKeys(streamNodeIds);
  console.log('stream_table_keys', streamKeys);
  console.log('descriptions', await kb.findDescriptionPaths(streamKeys));
  await kb.clearStreamData(streamKeys[0]);
  await kb.pushStreamData(streamKeys[0], { prop1: 'val1', prop2: 'val2' });
  console.log('list_stream_data', await kb.listStreamData(streamKeys[0]));
  // use native Date for filtering
  const fifteenMinutesAgo = new Date(Date.now() - 15 * 60 * 1000);
  const now = new Date();
  console.log('filtered', await kb.listStreamData(streamKeys[0], undefined, 0, fifteenMinutesAgo, now));

  // === RPC CLIENT/SERVER TEST ===
  console.log('***************************  RPC Client Functions ***************************');
  const clientNodeIds = await kb.rpcClientFindRpcClientIds(undefined, undefined, undefined, undefined);
  console.log('rpc_client_node_ids', clientNodeIds);
  await testClientQueue(kb, kb.rpcClientFindRpcClientKeys(clientNodeIds)[0]);
  console.log('***************************  RPC Server Functions ***************************');
  const serverNodeIds = await kb.rpcServerFindIds(undefined, undefined, undefined, undefined);
  console.log('rpc_server_node_ids', serverNodeIds);
  await testServerFunctions(kb, kb.rpcServerFindTableKeys(serverNodeIds)[0]);

  // === LINK TABLES TEST ===
  console.log('***************************  Link Tables ***************************');
  await kb.clearFilters(); kb.searchStartingPath('kb1.header1_link.header1_name');
  console.log('starting_path results', await kb.executeKbSearch());
  await kb.clearFilters(); kb.searchHasLink(); console.log('has_link results', await kb.executeKbSearch());
  console.log('link_names', await kb.linkTableFindAllLinkNames());
  console.log('node_names', await kb.linkTableFindAllNodeNames());

  // === LINK MOUNT TABLE TEST ===
  await kb.clearFilters(); kb.searchHasLinkMount(); console.log('has_link_mount', await kb.executeKbSearch());
  console.log('link_mount_names', await kb.linkMountTableFindAllLinkNames());
  console.log('mount_paths', await kb.linkMountTableFindAllMountPaths());

  await kb.querySupport.disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
