// testConstructDataTables.ts
import promptSync from 'prompt-sync';
import { ConstructDataTables } from './ConstructDataTables';

async function main() {
  const prompt = promptSync({ sigint: true });
  const password = prompt('Enter PostgreSQL password: ');

  // Database connection parameters
  const DB_HOST     = 'localhost';
  const DB_PORT     = 5432;
  const DB_NAME     = 'knowledge_base';
  const DB_USER     = 'gedgar';
  const DB_PASSWORD = password;
  const DATABASE    = 'knowledge_base';

  // Instantiate and initialize
  const builder = new ConstructDataTables(
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DATABASE,
  );
  await builder.init();

  console.log('Initial state:');
  console.log('Path:', builder.path);

  // Step through your Python example
  await builder.addKb('kb1', 'First knowledge base');
  builder.selectKb('kb1');
  await builder.addHeaderNode('header1_link', 'header1_name', { prop1: 'val1' }, { data: 'header1_data' });
  console.log('\nAfter add_header_node:');
  console.log('Path:', builder.path);

  await builder.addInfoNode('info1_link', 'info1_name', { prop2: 'val2' }, { data: 'info1_data' });
  console.log('\nAfter add_info_node:');
  console.log('Path:', builder.path);

  await builder.addRpcServerField('info1_server', 25, 'info1_server_data');
  await builder.addStatusField('info1_status', { prop3: 'val3' }, 'info1_status_description', { prop3: 'val3' });
  await builder.addStatusField('info2_status', { prop3: 'val3' }, 'info2_status_description', { prop3: 'val3' });
  await builder.addStatusField('info3_status', { prop3: 'val3' }, 'info3_status_description', { prop3: 'val3' });
  await builder.addJobField('info1_job', 100, 'info1_job_description');
  await builder.addStreamField('info1_stream', 95, 'info1_stream');
  await builder.addRpcClientField('info1_client', 10, 'info1_client_description');
  await builder.addLinkMount('info1_link_mount', 'info1_link_mount_description');
  builder.leaveHeaderNode('header1_link', 'header1_name');
  console.log('\nAfter leave_header_node:');
  console.log('Path:', builder.path);

  await builder.addHeaderNode('header2_link', 'header2_name', { prop3: 'val3' }, { data: 'header2_data' });
  await builder.addInfoNode('info2_link', 'info2_name', { prop4: 'val4' }, { data: 'info2_data' });
  await builder.addLinkNode('info1_link_mount');
  builder.leaveHeaderNode('header2_link', 'header2_name');
  console.log('\nAfter adding and leaving another header node:');
  console.log('Path:', builder.path);

  // Check installation
  try {
    await builder.checkInstallation();
    await builder.disconnect();
  } catch (err: any) {
    console.error('Error during installation check:', err.message);
  }
}

main().catch(err => {
  console.error('Test driver encountered an unexpected error:', err);
  process.exit(1);
});

