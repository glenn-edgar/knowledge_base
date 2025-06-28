"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KBSearch = void 0;
const pg_1 = require("pg");
class KBSearch {
    constructor(host, port, dbName, user, password, baseTable) {
        this.path = [];
        this.filters = [];
        this.results = null;
        this.pathValues = {};
        this.client = null;
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        this.baseTable = baseTable;
        this.linkTable = `${baseTable}_link`;
        this.linkMountTable = `${baseTable}_link_mount`;
        // fire-and-forget connect; errors will still bubble
        this._connect().catch(err => {
            console.error('Error connecting to database:', err);
            throw err;
        });
    }
    async _connect() {
        this.client = new pg_1.Client({
            host: this.host,
            port: this.port,
            database: this.dbName,
            user: this.user,
            password: this.password,
        });
        await this.client.connect();
    }
    async disconnect() {
        if (this.client) {
            await this.client.end();
            this.client = null;
        }
    }
    /**
     * Throws if not connected.
     */
    getConn() {
        if (!this.client) {
            throw new Error('Not connected to database. Call _connect() first.');
        }
        return this.client;
    }
    clearFilters() {
        this.filters = [];
        this.results = null;
    }
    searchKb(kb) {
        this.filters.push({ condition: 'knowledge_base = #', params: [kb] });
    }
    searchLabel(label) {
        this.filters.push({ condition: 'label = #', params: [label] });
    }
    searchName(name) {
        this.filters.push({ condition: 'name = #', params: [name] });
    }
    searchPropertyKey(key) {
        // JSONB ? operator
        this.filters.push({ condition: 'properties::jsonb ? #', params: [key] });
    }
    searchPropertyValue(key, value) {
        const jsonObject = { [key]: value };
        this.filters.push({
            condition: 'properties::jsonb @> #::jsonb',
            params: [JSON.stringify(jsonObject)],
        });
    }
    searchStartingPath(startingPath) {
        this.filters.push({ condition: 'path <@ #', params: [startingPath] });
    }
    searchPath(pathExpr) {
        this.filters.push({ condition: 'path ~ #', params: [pathExpr] });
    }
    searchHasLink() {
        this.filters.push({ condition: 'has_link = TRUE', params: [] });
    }
    searchHasLinkMount() {
        this.filters.push({ condition: 'has_link_mount = TRUE', params: [] });
    }
    /**
     * Builds and runs a WITH‐CTE chain of filters, returning all columns.
     */
    async executeQuery() {
        const client = this.getConn();
        const columnStr = '*';
        // no filters → simple SELECT
        if (this.filters.length === 0) {
            const res = await client.query(`SELECT ${columnStr} FROM ${this.baseTable}`);
            this.results = res.rows;
            return this.results;
        }
        // build CTEs
        const cteParts = [
            `base_data AS (SELECT ${columnStr} FROM ${this.baseTable})`,
        ];
        const combinedParams = [];
        let paramCount = 0;
        this.filters.forEach((filt, i) => {
            let cond = filt.condition;
            // for each param, replace one ‘#’ with $<n>
            filt.params.forEach(p => {
                paramCount++;
                const placeholder = `$${paramCount}`;
                cond = cond.replace('#', placeholder);
                combinedParams.push(p);
            });
            const cteName = `filter_${i}`;
            const prev = i === 0 ? 'base_data' : `filter_${i - 1}`;
            cteParts.push(`${cteName} AS (SELECT ${columnStr} FROM ${prev} WHERE ${cond})`);
        });
        const finalQuery = `WITH ${cteParts.join(',\n')}\nSELECT ${columnStr} FROM filter_${this.filters.length - 1}`;
        try {
            const res = await client.query(finalQuery, combinedParams);
            this.results = res.rows;
            return this.results;
        }
        catch (err) {
            console.error('Error executing query:', err);
            console.error('Query:', finalQuery);
            console.error('Parameters:', combinedParams);
            throw err;
        }
    }
    findPathValues(keyData) {
        if (!keyData)
            return [];
        const rows = Array.isArray(keyData) ? keyData : [keyData];
        return rows.map(r => r.path);
    }
    getResults() {
        return this.results || [];
    }
    findDescription(keyData) {
        const rows = Array.isArray(keyData) ? keyData : [keyData];
        return rows.map(r => {
            const props = r.properties || {};
            return { [r.path]: props.description || '' };
        });
    }
    /**
     * Fetches `data` for one or many paths in a single query.
     */
    async findDescriptionPaths(pathArray) {
        const client = this.getConn();
        const paths = Array.isArray(pathArray) ? pathArray : [pathArray];
        if (paths.length === 0)
            return {};
        let query;
        let params;
        if (paths.length === 1) {
            query = `SELECT path, data FROM ${this.baseTable} WHERE path = $1`;
            params = [paths[0]];
        }
        else {
            const placeholders = paths.map((_, i) => `$${i + 1}`).join(', ');
            query = `SELECT path, data FROM ${this.baseTable} WHERE path IN (${placeholders})`;
            params = paths;
        }
        try {
            const res = await client.query(query, params);
            const output = {};
            res.rows.forEach(r => {
                output[r.path] = r.data;
            });
            // fill missing
            paths.forEach(p => {
                if (!(p in output))
                    output[p] = null;
            });
            return output;
        }
        catch (err) {
            throw new Error(`Error retrieving data for paths: ${err}`);
        }
    }
    /**
     * Splits a link‐encoded LTREE into [kbName, [[link, name], …]].
     */
    decodeLinkNodes(path) {
        if (!path)
            throw new Error('Path must be a non-empty string');
        const parts = path.split('.');
        if (parts.length < 3) {
            throw new Error(`Path must have at least 3 elements (kb.link.name), got ${parts.length}`);
        }
        const rem = parts.length - 1;
        if (rem % 2 !== 0) {
            throw new Error(`After kb identifier, must have even number of elements (link/name pairs), got ${rem}`);
        }
        const kb = parts[0];
        const pairs = [];
        for (let i = 1; i < parts.length; i += 2) {
            pairs.push([parts[i], parts[i + 1]]);
        }
        return [kb, pairs];
    }
}
exports.KBSearch = KBSearch;
