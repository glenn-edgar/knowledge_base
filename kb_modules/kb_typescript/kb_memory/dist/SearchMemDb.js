"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SearchMemDB = void 0;
const BasicConstructDb_1 = require("./BasicConstructDb");
/**
 * TypeScript translation of Python SearchMemDB
 */
class SearchMemDB extends BasicConstructDb_1.BasicConstructDB {
    constructor(host, port, dbname, user, password, tableName) {
        super(host, port, dbname, user, password, tableName);
        this.kbs = {};
        this.labels = {};
        this.names = {};
        this.decodedKeys = {};
        this.filterResults = new Map();
        // Import existing data and initialize filters
        this.importFromPostgres(this.tableName)
            .then(() => {
            this.filterResults = new Map(this.data);
            this.generateDecodedKeys();
        })
            .catch(err => {
            throw new Error(`Failed to load data: ${err}`);
        });
    }
    generateDecodedKeys() {
        this.kbs = {};
        this.labels = {};
        this.names = {};
        this.decodedKeys = {};
        for (const key of this.data.keys()) {
            const parts = key.split('.');
            this.decodedKeys[key] = parts;
            const kb = parts[0];
            const label = parts[parts.length - 2];
            const name = parts[parts.length - 1];
            if (!this.kbs[kb])
                this.kbs[kb] = [];
            this.kbs[kb].push(key);
            if (!this.labels[label])
                this.labels[label] = [];
            this.labels[label].push(key);
            if (!this.names[name])
                this.names[name] = [];
            this.names[name].push(key);
        }
    }
    /** Clear all filters and reset to full data */
    clearFilters() {
        this.filterResults = new Map(this.data);
    }
    /** Filter by knowledge base */
    searchKb(kbName) {
        const result = new Map();
        const keys = this.kbs[kbName] || [];
        for (const key of keys) {
            if (this.filterResults.has(key)) {
                result.set(key, this.filterResults.get(key));
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Filter by label */
    searchLabel(label) {
        const result = new Map();
        const keys = this.labels[label] || [];
        for (const key of keys) {
            if (this.filterResults.has(key)) {
                result.set(key, this.filterResults.get(key));
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Filter by name */
    searchName(name) {
        const result = new Map();
        const keys = this.names[name] || [];
        for (const key of keys) {
            if (this.filterResults.has(key)) {
                result.set(key, this.filterResults.get(key));
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Filter by presence of property key */
    searchPropertyKey(dataKey) {
        const result = new Map();
        for (const [key, node] of this.filterResults) {
            if (node.data && dataKey in node.data) {
                result.set(key, node);
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Filter by property key/value match */
    searchPropertyValue(dataKey, dataValue) {
        const result = new Map();
        for (const [key, node] of this.filterResults) {
            if (node.data && node.data[dataKey] === dataValue) {
                result.set(key, node);
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Filter by starting path and its descendants */
    searchStartingPath(startingPath) {
        if (typeof startingPath !== 'string') {
            throw new Error('startingPath must be a string');
        }
        const result = new Map();
        // exact match
        if (this.filterResults.has(startingPath)) {
            result.set(startingPath, this.filterResults.get(startingPath));
        }
        // descendants
        const descendants = this.queryDescendants(startingPath);
        for (const row of descendants) {
            if (this.filterResults.has(row.path)) {
                result.set(row.path, this.filterResults.get(row.path));
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Filter by LTREE operator */
    searchPath(operator, pathExpr) {
        const searchResults = this.queryByOperator(operator, pathExpr);
        const result = new Map();
        for (const item of searchResults) {
            if (this.filterResults.has(item.path)) {
                result.set(item.path, this.filterResults.get(item.path));
            }
        }
        this.filterResults = result;
        return this.filterResults;
    }
    /** Extract description for each node */
    findDescriptions() {
        const descs = {};
        for (const [key, node] of this.data) {
            descs[key] = node.data.description || '';
        }
        return descs;
    }
}
exports.SearchMemDB = SearchMemDB;
