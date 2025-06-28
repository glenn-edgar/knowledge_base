// testDriver.ts

import promptSync from 'prompt-sync';
import { KnowledgeBaseManager, ConnectionParams } from './KnowledgeBaseManager';

async function main() {
  const prompt = promptSync();
  const password = prompt('Enter PostgreSQL password: ');

  // Database connection parameters
  const connParams: ConnectionParams = {
    host: 'localhost',
    database: 'knowledge_base',
    user: 'gedgar',
    password,
    port: 5432,
  };

  // Instantiate and initialize (connect + create tables)
  const kbManager = new KnowledgeBaseManager('knowledge_base', connParams);
  await kbManager.init();

  console.log('starting unit test');

  // Add knowledge bases
  await kbManager.addKb('kb1', 'First knowledge base');
  await kbManager.addKb('kb2', 'Second knowledge base');

  // Add nodes
  await kbManager.addNode(
    'kb1',
    'person',
    'John Doe',
    { age: 30 },
    { email: 'john@example.com' },
    'people.john'
  );
  await kbManager.addNode(
    'kb2',
    'person',
    'Jane Smith',
    { age: 25 },
    { email: 'jane@example.com' },
    'people.jane'
  );

  // Create a link‐mount on John’s node
  await kbManager.addLinkMount(
    'kb1',
    'people.john',
    'link1',
    'link1 description'
  );

  // Add a link record for that mount
  await kbManager.addLink('kb1', 'people.john', 'link1');

  // Tear down
  await kbManager.disconnect();
  console.log('ending unit test');
}

main().catch(err => {
  console.error('Test driver encountered an error:', err);
  process.exit(1);
});

