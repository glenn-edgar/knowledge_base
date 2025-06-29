"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// testDriver.ts
const prompt_sync_1 = __importDefault(require("prompt-sync"));
const BasicConstructDb_1 = require("./BasicConstructDb");
async function main() {
    // Prompt for PostgreSQL password
    const prompt = (0, prompt_sync_1.default)();
    const password = prompt('Enter PostgreSQL password: ');
    // Initialize the tree storage system
    const tree = new BasicConstructDb_1.BasicConstructDB('localhost', // host
    5432, // port
    'knowledge_base', // dbname
    'gedgar', // user
    password, // password
    'knowledge_base' // tableName
    );
    console.log('\n=== Full ltree-Compatible Tree Storage System ===\n');
    // Sample data setup
    const sampleData = [
        ['company', { name: 'TechCorp', type: 'corporation' }],
        ['company.engineering', { name: 'Engineering', type: 'department' }],
        ['company.engineering.backend', { name: 'Backend Team', type: 'team' }],
        ['company.engineering.backend.api', { name: 'API Service', type: 'service' }],
        ['company.engineering.backend.database', { name: 'Database Team', type: 'service' }],
        ['company.engineering.frontend', { name: 'Frontend Team', type: 'team' }],
        ['company.engineering.frontend.web', { name: 'Web App', type: 'service' }],
        ['company.engineering.frontend.mobile', { name: 'Mobile App', type: 'service' }]
    ];
    // Store sample data
    for (const [path, data] of sampleData) {
        tree.store(path, data);
    }
    // List all stored nodes
    console.log('All stored nodes:');
    for (const node of tree.query('*')) {
        console.log(`  ${node.path} →`, node.data);
    }
    // Retrieve a single node
    console.log('\nRetrieve single node at "company.engineering.backend.api":');
    console.log('  ', tree.get('company.engineering.backend.api'));
    // Query with an ltree pattern
    console.log('\nQuery pattern "company.engineering.*.database":');
    for (const node of tree.query('company.engineering.*.database')) {
        console.log(`  ${node.path}`);
    }
    // Ancestors of a given node
    const target = 'company.engineering.backend.api';
    console.log(`\nAncestors of "${target}":`);
    for (const node of tree.queryAncestors(target)) {
        console.log(`  ${node.path}`);
    }
    // Descendants of a given node
    const parent = 'company.engineering';
    console.log(`\nDescendants of "${parent}":`);
    for (const node of tree.queryDescendants(parent)) {
        console.log(`  ${node.path}`);
    }
    // Lowest common ancestor
    const a = 'company.engineering.backend.api';
    const b = 'company.engineering.backend.database';
    console.log(`\nLowest common ancestor of "${a}" and "${b}":\n  `, tree.lca(a, b));
    await tree.close();
}
main().catch(err => {
    console.error('Error in test driver:', err);
    process.exit(1);
});
