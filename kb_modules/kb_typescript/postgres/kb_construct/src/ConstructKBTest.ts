// testConstructKB.ts
import promptSync from 'prompt-sync';
import { ConstructKB } from './ConstructKB';

async function main() {
  const prompt = promptSync({ sigint: true });

  // Database connection parameters
  const DB_HOST     = 'localhost';
  const DB_PORT     = 5432;
  const DB_NAME     = 'knowledge_base';
  const DB_USER     = 'gedgar';
  const DB_PASSWORD = prompt('Enter PostgreSQL password: ');
  const DB_TABLE    = 'knowledge_base';

  console.log('starting unit test');
  const kb = new ConstructKB(
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_TABLE,
  );
  await kb.init();

  // KB #1
  await kb.addKb('kb1', 'First knowledge base');
  kb.selectKb('kb1');
  await kb.addHeaderNode(
    'header1_link',
    'header1_name',
    { prop1: 'val1' },
    { data: 'header1_data' },
  );
  await kb.addInfoNode(
    'info1_link',
    'info1_name',
    { prop2: 'val2' },
    { data: 'info1_data' },
  );
  kb.leaveHeaderNode('header1_link', 'header1_name');

  await kb.addHeaderNode(
    'header2_link',
    'header2_name',
    { prop3: 'val3' },
    { data: 'header2_data' },
  );
  await kb.addInfoNode(
    'info2_link',
    'info2_name',
    { prop4: 'val4' },
    { data: 'info2_data' },
  );
  await kb.addLinkMount('link1', 'link1 description');
  kb.leaveHeaderNode('header2_link', 'header2_name');

  // KB #2
  await kb.addKb('kb2', 'Second knowledge base');
  kb.selectKb('kb2');
  await kb.addHeaderNode(
    'header1_link',
    'header1_name',
    { prop1: 'val1' },
    { data: 'header1_data' },
  );
  await kb.addInfoNode(
    'info1_link',
    'info1_name',
    { prop2: 'val2' },
    { data: 'info1_data' },
  );
  kb.leaveHeaderNode('header1_link', 'header1_name');

  await kb.addHeaderNode(
    'header2_link',
    'header2_name',
    { prop3: 'val3' },
    { data: 'header2_data' },
  );
  await kb.addInfoNode(
    'info2_link',
    'info2_name',
    { prop4: 'val4' },
    { data: 'info2_data' },
  );
  await kb.addLinkNode('link1');
  kb.leaveHeaderNode('header2_link', 'header2_name');

  // Installation check + teardown
  try {
    await kb.checkInstallation();
  } catch (err: any) {
    console.error(`Error during installation check: ${err.message}`);
  } finally {
    await kb.disconnect();
  }

  console.log('ending unit test');
}

main().catch(err => {
  console.error('Test driver encountered an unexpected error:', err);
  process.exit(1);
});
