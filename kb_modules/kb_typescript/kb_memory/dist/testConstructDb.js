"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const prompt_sync_1 = __importDefault(require("prompt-sync"));
const ConstructMemDb_1 = require("./ConstructMemDb");
async function main() {
    const prompt = (0, prompt_sync_1.default)({ sigint: true });
    const password = prompt('Enter PostgreSQL password: ');
    const DB_HOST = 'localhost';
    const DB_PORT = 5432;
    const DB_NAME = 'knowledge_base';
    const DB_USER = 'gedgar';
    const DB_PASSWORD = password;
    const DB_TABLE = 'knowledge_base';
    console.log('starting unit test');
    const kb = new ConstructMemDb_1.ConstructMemDB(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_TABLE);
    // KB1 setup
    kb.addKb('kb1', 'First knowledge base');
    kb.selectKb('kb1');
    kb.addHeaderNode('header1_link', 'header1_name', { data: 'header1_data' }, 'header1_description');
    kb.addInfoNode('info1_link', 'info1_name', { data: 'info1_data' }, 'info1_description');
    kb.leaveHeaderNode('header1_link', 'header1_name');
    kb.addHeaderNode('header2_link', 'header2_name', { data: 'header2_data' }, 'header2_description');
    kb.addInfoNode('info2_link', 'info2_name', { data: 'info2_data' }, 'info2_description');
    kb.leaveHeaderNode('header2_link', 'header2_name');
    // KB2 setup
    kb.addKb('kb2', 'Second knowledge base');
    kb.selectKb('kb2');
    kb.addHeaderNode('header1_link', 'header1_name', { data: 'header1_data' }, 'header1_description');
    kb.addInfoNode('info1_link', 'info1_name', { data: 'info1_data' }, 'info1_description');
    kb.leaveHeaderNode('header1_link', 'header1_name');
    kb.addHeaderNode('header2_link', 'header2_name', { data: 'header2_data' }, 'header2_description');
    kb.addInfoNode('info2_link', 'info2_name', { data: 'info2_data' }, 'info2_description');
    kb.leaveHeaderNode('header2_link', 'header2_name');
    // Installation check and sync
    try {
        if (kb.checkInstallation()) {
            const exported = await kb.exportToPostgres('composite_memory_kb', true, true);
            console.log(`Exported ${exported} records to composite_memory_kb`);
            const imported = await kb.importFromPostgres('composite_memory_kb');
            console.log(`Imported ${imported} records from composite_memory_kb`);
        }
    }
    catch (err) {
        console.error(`Error during installation check: ${err}`);
    }
    finally {
        await kb.pool.end();
    }
    console.log('ending unit test');
}
main().catch(console.error);
