"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BasicConstructDB = void 0;
const pg_1 = require("pg");
class BasicConstructDB {
    constructor(host, port, dbname, user, password, tableName) {
        this.data = new Map();
        this.kbDict = {};
        this.pool = new pg_1.Pool({ host, port, database: dbname, user, password });
        this.tableName = tableName;
    }
    addKb(kbName, description = "") {
        if (this.kbDict[kbName]) {
            throw new Error(`Knowledge base ${kbName} already exists`);
        }
        this.kbDict[kbName] = { description };
    }
    validatePath(path) {
        if (!path)
            return false;
        const pattern = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/;
        if (!pattern.test(path))
            return false;
        return path.split('.').every(label => label.length >= 1 && label.length <= 256);
    }
    pathLabels(path) {
        return path.split('.');
    }
    subpath(path, start, length) {
        const labels = this.pathLabels(path);
        if (start < 0)
            start = labels.length + start;
        return length == null
            ? labels.slice(start).join('.')
            : labels.slice(start, start + length).join('.');
    }
    escapeRegex(s) {
        return s.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&');
    }
    convertLqueryPattern(pattern) {
        let result = this.escapeRegex(pattern);
        // *{n,m}
        result = result.replace(/\\\*\\\{(\d+),(\d+)\\\}/g, (_m, n, m2) => `([^.]+\\.){${n},${m2}}`);
        // *{n,}
        result = result.replace(/\\\*\\\{(\d+),\\\}/g, (_m, n) => `([^.]+\\.){${n},}`);
        // *{,m}
        result = result.replace(/\\\*\\\{,(\d+)\\\}/g, (_m, m2) => `([^.]+\\.){0,${m2}}`);
        // *{n}
        result = result.replace(/\\\*\\\{(\d+)\\\}/g, (_m, n) => `([^.]+\\.){${n}}`);
        // ** -> .*  
        result = result.replace(/\\\*\\\*/g, '.*');
        // * -> [^.]+
        result = result.replace(/\\\*/g, '[^.]+');
        // {a,b,c}
        result = result.replace(/\\\{([^}]+)\\\}/g, (_m, grp) => `(${grp.split(',').join('|')})`);
        return `^${result}$`;
    }
    convertSimplePattern(pattern) {
        const parts = pattern.split('.*');
        const escaped = parts.map(p => this.escapeRegex(p)).join('.*');
        let result = escaped
            .replace(/\\\*\\\*/g, '.*')
            .replace(/\\\*/g, '[^.]+')
            .replace(/\\\{([^}]+)\\\}/g, (_m, grp) => `(${grp.split(',').join('|')})`);
        return `^${result}$`;
    }
    convertLtreeQueryToRegex(query) {
        if (query.includes('@') && !query.startsWith('@') && !query.endsWith('@')) {
            return this.convertSimplePattern(query.replace(/@/g, '.'));
        }
        return this.convertLqueryPattern(query);
    }
    ltreeMatch(path, query) {
        try {
            const regex = new RegExp(this.convertLtreeQueryToRegex(query));
            return regex.test(path);
        }
        catch {
            return false;
        }
    }
    ltxtqueryMatch(path, expr) {
        const pathWords = new Set(path.split('.'));
        let q = expr.trim();
        if (!/[&|!]/.test(q))
            return pathWords.has(q);
        q = q.replace(/&/g, ' && ').replace(/\|/g, ' || ').replace(/!/g, ' !');
        const tokens = expr.match(/\w+/g) || [];
        for (const word of tokens) {
            if (!['and', 'or', 'not'].includes(word)) {
                q = q.replace(new RegExp(`\\b${word}\\b`, 'g'), `pathWords.has('${word}')`);
            }
        }
        try {
            return eval(q);
        }
        catch {
            return false;
        }
    }
    ltreeAncestor(ancestor, descendant) {
        return ancestor !== descendant && descendant.startsWith(`${ancestor}.`);
    }
    ltreeDescendant(desc, anc) {
        return this.ltreeAncestor(anc, desc);
    }
    ltreeAncestorOrEqual(anc, desc) {
        return anc === desc || this.ltreeAncestor(anc, desc);
    }
    ltreeDescendantOrEqual(desc, anc) {
        return desc === anc || this.ltreeDescendant(desc, anc);
    }
    ltreeConcatenate(p1, p2) {
        if (!p1)
            return p2;
        if (!p2)
            return p1;
        return `${p1}.${p2}`;
    }
    nlevel(path) { return path.split('.').length; }
    subltree(path, start, end) {
        return this.pathLabels(path).slice(start, end).join('.');
    }
    subpathFunc(path, offset, length) {
        return this.subpath(path, offset, length);
    }
    indexFunc(path, sub, offset = 0) {
        const labels = path.split('.'), sublabels = sub.split('.');
        for (let i = offset; i <= labels.length - sublabels.length; i++) {
            if (labels.slice(i, i + sublabels.length).join('.') === sublabels.join('.'))
                return i;
        }
        return -1;
    }
    lca(...paths) {
        if (paths.length === 0)
            return null;
        if (paths.length === 1)
            return paths[0];
        const lists = paths.map(p => p.split('.'));
        const minLen = Math.min(...lists.map(l => l.length));
        const common = [];
        for (let i = 0; i < minLen; i++) {
            const label = lists[0][i];
            if (lists.every(l => l[i] === label))
                common.push(label);
            else
                break;
        }
        return common.length ? common.join('.') : null;
    }
    store(path, data, createdAt, updatedAt) {
        if (!this.validatePath(path))
            throw new Error(`Invalid path: ${path}`);
        this.data.set(path, { path, data: JSON.parse(JSON.stringify(data)), createdAt, updatedAt });
        return true;
    }
    get(path) {
        if (!this.validatePath(path))
            throw new Error(`Invalid path: ${path}`);
        const node = this.data.get(path);
        return node ? JSON.parse(JSON.stringify(node.data)) : null;
    }
    getNode(path) {
        if (!this.validatePath(path))
            throw new Error(`Invalid path: ${path}`);
        const node = this.data.get(path);
        return node ? { ...node } : null;
    }
    query(pattern) {
        const results = [];
        for (const node of this.data.values()) {
            if (this.ltreeMatch(node.path, pattern))
                results.push({ ...node, data: JSON.parse(JSON.stringify(node.data)) });
        }
        return results.sort((a, b) => a.path.localeCompare(b.path));
    }
    queryLtxt(expr) {
        const results = [];
        for (const node of this.data.values()) {
            if (this.ltxtqueryMatch(node.path, expr))
                results.push({ ...node, data: JSON.parse(JSON.stringify(node.data)) });
        }
        return results.sort((a, b) => a.path.localeCompare(b.path));
    }
    queryAncestors(path) {
        if (!this.validatePath(path))
            throw new Error(`Invalid path: ${path}`);
        const res = [];
        for (const node of this.data.values()) {
            if (this.ltreeAncestor(node.path, path))
                res.push({ ...node, data: JSON.parse(JSON.stringify(node.data)) });
        }
        return res.sort((a, b) => a.path.split('.').length - b.path.split('.').length);
    }
    queryDescendants(path) {
        if (!this.validatePath(path))
            throw new Error(`Invalid path: ${path}`);
        const res = [];
        for (const node of this.data.values()) {
            if (this.ltreeDescendant(node.path, path))
                res.push({ ...node, data: JSON.parse(JSON.stringify(node.data)) });
        }
        return res.sort((a, b) => a.path.localeCompare(b.path));
    }
    querySubtree(path) {
        const res = this.getNode(path) ? [this.getNode(path)] : [];
        return res.concat(this.queryDescendants(path));
    }
    exists(path) { return this.data.has(path) && this.validatePath(path); }
    delete(path) { return this.data.delete(path); }
    addSubtree(path, subtree) {
        if (!this.exists(path))
            throw new Error(`Path ${path} not found`);
        subtree.forEach(n => this.store(`${path}.${n.path}`, n.data));
        return true;
    }
    deleteSubtree(path) {
        const toDel = Array.from(this.data.keys()).filter(p => p === path || this.ltreeDescendant(p, path));
        toDel.forEach(p => this.data.delete(p));
        return toDel.length;
    }
    // PostgreSQL integration
    async withClient(fn) {
        const client = await this.pool.connect();
        try {
            return await fn(client);
        }
        finally {
            client.release();
        }
    }
    async importFromPostgres(pathCol = 'path', dataCol = 'data', createdCol = 'created_at', updatedCol = 'updated_at') {
        return this.withClient(async (client) => {
            const exist = await client.query(`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=$1) as exists`, [this.tableName]);
            if (!exist.rows[0].exists)
                throw new Error(`Table ${this.tableName} not found`);
            const res = await client.query(`SELECT ${pathCol}::text as path, ${dataCol}, ${createdCol}::text as created_at, ${updatedCol}::text as updated_at FROM ${this.tableName} ORDER BY ${pathCol}`);
            let count = 0;
            for (const row of res.rows) {
                const data = typeof row[dataCol] === 'string' ? JSON.parse(row[dataCol]) : row[dataCol];
                this.store(row.path, data, row.created_at, row.updated_at);
                count++;
            }
            return count;
        });
    }
    async exportToPostgres(createTable = true, clearExisting = false) {
        return this.withClient(async (client) => {
            await client.query('CREATE EXTENSION IF NOT EXISTS ltree');
            if (createTable) {
                await client.query(`CREATE TABLE IF NOT EXISTS ${this.tableName} (id SERIAL PRIMARY KEY, path LTREE UNIQUE NOT NULL, data JSONB, created_at TIMESTAMP, updated_at TIMESTAMP)`);
                await client.query(`CREATE INDEX IF NOT EXISTS ${this.tableName}_path_idx ON ${this.tableName} USING GIST(path)`);
            }
            if (clearExisting) {
                await client.query(`TRUNCATE ${this.tableName}`);
            }
            let count = 0;
            for (const node of this.data.values()) {
                try {
                    await client.query(`INSERT INTO ${this.tableName} (path, data, created_at, updated_at) VALUES ($1,$2,$3,$4) ON CONFLICT(path) DO UPDATE SET data=EXCLUDED.data, updated_at=EXCLUDED.updated_at`, [node.path, node.data, node.createdAt, node.updatedAt]);
                    count++;
                }
                catch { }
            }
            return count;
        });
    }
    async syncWithPostgres(direction = 'both') {
        const stats = { imported: 0, exported: 0 };
        if (direction === 'import' || direction === 'both') {
            stats.imported = await this.importFromPostgres();
        }
        if (direction === 'export' || direction === 'both') {
            stats.exported = await this.exportToPostgres();
        }
        return stats;
    }
    getStats() {
        const paths = Array.from(this.data.keys());
        const depths = paths.map(p => this.nlevel(p));
        const total = paths.length;
        if (!total)
            return { total, maxDepth: 0, avgDepth: 0, root: 0, leaves: 0 };
        const maxDepth = Math.max(...depths);
        const avgDepth = depths.reduce((a, b) => a + b, 0) / total;
        const root = depths.filter(d => d === 1).length;
        const leaves = paths.filter(p => !paths.some(q => this.ltreeAncestor(p, q))).length;
        return { total, maxDepth, avgDepth, root, leaves };
    }
    clear() { this.data.clear(); }
    size() { return this.data.size; }
    getAllPaths() { return Array.from(this.data.keys()).sort(); }
}
exports.BasicConstructDB = BasicConstructDB;
