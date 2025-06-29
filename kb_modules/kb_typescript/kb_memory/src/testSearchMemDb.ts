import promptSync from 'prompt-sync';
import { SearchMemDB } from './SearchMemDb';

async function main() {
  const prompt = promptSync({ sigint: true });
  const password = prompt('Enter Postgres Password: ');

  // Instantiate the search-enabled memory DB
  const kb = new SearchMemDB(
    'localhost',
    5432,
    'knowledge_base',
    'gedgar',
    password,
    'composite_memory_kb'
  );

  // Load data from Postgres and build indices
  await kb.importFromPostgres('composite_memory_kb');
  (kb as any).generateDecodedKeys();

  console.log('decodedKeys:', Object.keys(kb.decodedKeys));

  // Filter operations
  kb.clearFilters();
  console.log('----------------------------------');
  kb.searchKb('kb1');
  console.log('searchKb:', Array.from(kb.filterResults.keys()));

  console.log('----------------------------------');
  kb.searchLabel('info1_link');
  console.log('searchLabel:', Array.from(kb.filterResults.keys()));

  console.log('----------------------------------');
  kb.searchName('info1_name');
  console.log('searchName:', Array.from(kb.filterResults.keys()));

  console.log('----------------------------------');
  kb.clearFilters();
  console.log(
    'searchPropertyValue:',
    Array.from(kb.searchPropertyValue('data', 'info1_data').keys())
  );

  console.log('----------------------------------');
  kb.clearFilters();
  console.log('searchPropertyKey:', Array.from(kb.searchPropertyKey('data').keys()));

  console.log('----------------------------------');
  kb.clearFilters();
  console.log(
    'searchStartingPath:',
    Array.from(kb.searchStartingPath('kb2.header2_link.header2_name').keys())
  );

  console.log('----------------------------------');
  kb.clearFilters();
  console.log('searchPath:', Array.from(kb.searchPath('~', 'kb2.**').keys()));

  console.log('----------------------------------');
  kb.clearFilters();
  console.log('findDescriptions:', kb.findDescriptions());
  console.log('----------------------------------');

  await kb.close();
}

main().catch(console.error);
