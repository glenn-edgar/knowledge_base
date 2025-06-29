"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BasicConstructDB = void 0;
const pg_1 = require("pg");
class BasicConstructDB {
    constructor(host, port, dbname, user, password, tableName) {
        this.data = new Map();
        this.kbDict = new Map();
        this.tableName = tableName;
        this.connectionParams = {
            host,
            port,
            database: dbname,
            user,
            password
        };
        this.pool = new pg_1.Pool(this.connectionParams);
    }
    addKb(kbName, description = "") {
        if (this.kbDict.has(kbName)) {
            throw new Error(`Knowledge base ${kbName} already exists`);
        }
        this.kbDict.set(kbName, { description });
    }
    validatePath(path) {
        if (!path)
            return false;
        const pattern = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/;
        if (!pattern.test(path))
            return false;
        return path.split('.').every(label => label.length >= 1 && label.length <= 256);
    }
    pathDepth(path) {
        return path.split('.').length;
    }
    pathLabels(path) {
        return path.split('.');
    }
    subpath(path, start, length) {
        const labels = this.pathLabels(path);
        if (start < 0)
            start = labels.length + start;
        return length === undefined
            ? labels.slice(start).join('.')
            : labels.slice(start, start + length).join('.');
    }
    escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&');
    }
    convertLtreeQueryToRegex(query) {
        if (query.includes('@') && !query.startsWith('@') && !query.endsWith('@')) {
            return this.convertSimplePattern(query.replace(/@/g, '.'));
        }
        return this.convertLqueryPattern(query);
    }
    convertLqueryPattern(pattern) {
        let result = this.escapeRegex(pattern);
        result = result.replace(/\\\*\\\{(\d+),(\d+)\\\}/g, (_, n, m) => `([^.]+\\.){${n},${m}}`);
        result = result.replace(/\\\*\\\{(\d+),\\\}/g, (_, n) => `([^.]+\\.){${n},}`);
        result = result.replace(/\\\*\\\{,(\d+)\\\}/g, (_, m) => `([^.]+\\.){0,${m}}`);
        result = result.replace(/\\\*\\\{(\d+)\\\}/g, (_, n) => `([^.]+\\.){${n}}`);
        result = result.replace(/\\\*\\\*/g, '.*');
        result = result.replace(/\\\*/g, '[^.]+');
        result = result.replace(/\\\{([^}]+)\\\}/g, (_, grp) => `(${grp.split(',').join('|')})`);
        result = result.replace(/\\\.\)\{([^}]+)\}/g, '){$1}[^.]*');
        return `^${result}$`;
    }
    convertSimplePattern(pattern) {
        const parts = pattern.split('.*');
        const escapedParts = parts.map(part => this.escapeRegex(part));
        let result = escapedParts.join('.*');
        result = result.replace(/\\\*\\\*/g, '.*');
        result = result.replace(/\\\*/g, '[^.]+');
        result = result.replace(/\\\{([^}]+)\\\}/g, (_, grp) => `(${grp.split(',').join('|')})`);
        return `^${result}$`;
    }
    ltreeMatch(path, query) {
        try {
            const regexPattern = this.convertLtreeQueryToRegex(query);
            return new RegExp(regexPattern).test(path);
        }
        catch {
            return false;
        }
    }
    ltxtqueryMatch(path, ltxtquery) {
        const pathWords = new Set(path.split('.'));
        let query = ltxtquery.trim();
        if (!query.includes('&') && !query.includes('|') && !query.includes('!')) {
            return pathWords.has(query);
        }
        query = query.replace(/&/g, ' && ')
            .replace(/\|/g, ' || ')
            .replace(/!/g, ' !');
        const wordRegex = /\b\w+\b/g;
        const words = ltxtquery.match(wordRegex) || [];
        for (const word of words) {
            if (!['and', 'or', 'not'].includes(word.toLowerCase())) {
                const regex = new RegExp(`\\b${this.escapeRegex(word)}\\b`, 'g');
                query = query.replace(regex, pathWords.has(word) ? 'true' : 'false');
            }
        }
        try {
            return new Function(`return ${query}`)();
        }
        catch {
            return false;
        }
    }
    ltreeAncestor(ancestor, descendant) {
        if (ancestor === descendant)
            return false;
        return descendant.startsWith(ancestor + '.');
    }
    ltreeDescendant(descendant, ancestor) {
        return this.ltreeAncestor(ancestor, descendant);
    }
    ltreeAncestorOrEqual(ancestor, descendant) {
        return ancestor === descendant || this.ltreeAncestor(ancestor, descendant);
    }
    ltreeDescendantOrEqual(descendant, ancestor) {
        return descendant === ancestor || this.ltreeDescendant(descendant, ancestor);
    }
    ltreeConcatenate(path1, path2) {
        if (!path1)
            return path2;
        if (!path2)
            return path1;
        return `${path1}.${path2}`;
    }
    nlevel(path) {
        return path.split('.').length;
    }
    subltree(path, start, end) {
        const labels = path.split('.');
        return labels.slice(start, end).join('.');
    }
    subpathFunc(path, offset, length) {
        return this.subpath(path, offset, length);
    }
    indexFunc(path, subpath, offset = 0) {
        const labels = path.split('.');
        const subLabels = subpath.split('.');
        for (let i = offset; i <= labels.length - subLabels.length; i++) {
            if (labels.slice(i, i + subLabels.length).join('.') === subpath) {
                return i;
            }
        }
        return -1;
    }
    text2ltree(text) {
        if (this.validatePath(text))
            return text;
        throw new Error(`Cannot convert '${text}' to valid ltree format`);
    }
    ltree2text(ltreePath) {
        return ltreePath;
    }
    lca(...paths) {
        if (!paths.length)
            return null;
        if (paths.length === 1)
            return paths[0];
        const allLabels = paths.map(p => p.split('.'));
        const minLength = Math.min(...allLabels.map(l => l.length));
        const common = [];
        for (let i = 0; i < minLength; i++) {
            const label = allLabels[0][i];
            if (allLabels.every(l => l[i] === label))
                common.push(label);
            else
                break;
        }
        return common.length ? common.join('.') : null;
    }
    store(path, data, createdAt, updatedAt) {
        if (!this.validatePath(path)) {
            throw new Error(`Invalid ltree path: ${path}`);
        }
        this.data.set(path, {
            path,
            data: this.deepClone(data),
            createdAt: createdAt || null,
            updatedAt: updatedAt || null
        });
        return true;
    }
    get(path) {
        if (!this.validatePath(path)) {
            throw new Error(`Invalid ltree path: ${path}`);
        }
        const node = this.data.get(path);
        return node ? this.deepClone(node.data) : null;
    }
    getNode(path) {
        if (!this.validatePath(path)) {
            throw new Error(`Invalid ltree path: ${path}`);
        }
        const node = this.data.get(path);
        return node ? this.deepClone(node) : null;
    }
    query(pattern) {
        const res = [];
        this.data.forEach((node, p) => {
            if (this.ltreeMatch(p, pattern)) {
                res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
            }
        });
        return res.sort((a, b) => a.path.localeCompare(b.path));
    }
    queryLtxtquery(ltxtquery) {
        const res = [];
        this.data.forEach((node, p) => {
            if (this.ltxtqueryMatch(p, ltxtquery)) {
                res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
            }
        });
        return res.sort((a, b) => a.path.localeCompare(b.path));
    }
    queryByOperator(operator, path1, path2) {
        const res = [];
        if (operator === '@>') {
            this.data.forEach((node, p) => {
                if (this.ltreeAncestor(p, path1))
                    res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
            });
        }
        else if (operator === '<@') {
            this.data.forEach((node, p) => {
                if (this.ltreeDescendant(p, path1))
                    res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
            });
        }
        else if (operator === '~') {
            return this.query(path1);
        }
        else if (operator === '@@') {
            return this.queryLtxtquery(path1);
        }
        return res.sort((a, b) => a.path.localeCompare(b.path));
    }
    queryAncestors(path) {
        if (!this.validatePath(path))
            throw new Error(`Invalid ltree path: ${path}`);
        const res = [];
        this.data.forEach((node, p) => {
            if (this.ltreeAncestor(p, path))
                res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
        });
        return res.sort((a, b) => this.nlevel(a.path) - this.nlevel(b.path));
    }
    queryDescendants(path) {
        if (!this.validatePath(path))
            throw new Error(`Invalid ltree path: ${path}`);
        const res = [];
        this.data.forEach((node, p) => {
            if (this.ltreeDescendant(p, path))
                res.push({ path: p, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
        });
        return res.sort((a, b) => a.path.localeCompare(b.path));
    }
    querySubtree(path) {
        const res = [];
        if (this.exists(path)) {
            const node = this.data.get(path);
            res.push({ path, data: this.deepClone(node.data), created_at: node.createdAt, updated_at: node.updatedAt });
        }
        return [...res, ...this.queryDescendants(path)].sort((a, b) => a.path.localeCompare(b.path));
    }
    exists(path) {
        return this.data.has(path) && this.validatePath(path);
    }
    delete(path) {
        return this.data.delete(path);
    }
    addSubtree(path, subtree) {
        if (!this.validatePath(path))
            throw new Error(`Invalid ltree path: ${path}`);
        if (!this.exists(path))
            throw new Error(`Path ${path} does not exist`);
        for (const node of subtree) {
            this.store(`${path}.${node.path}`, node.data);
        }
        return true;
    }
    deleteSubtree(path) {
        const toDel = [];
        this.data.forEach((_, p) => {
            if (p === path || this.ltreeDescendant(p, path))
                toDel.push(p);
        });
        toDel.forEach(p => this.data.delete(p));
        return toDel.length;
    }
    async importFromPostgres(tableName = 'tree_data', pathColumn = 'path', dataColumn = 'data', createdAtColumn = 'created_at', updatedAtColumn = 'updated_at') {
        const client = await this.pool.connect();
        try {
            const tableCheck = await client.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables
          WHERE table_name = $1
        );
      `, [tableName]);
            if (!tableCheck.rows[0].exists) {
                throw new Error(`Table '${tableName}' does not exist`);
            }
            const result = await client.query(`
        SELECT
          ${pathColumn}::text as path,
          ${dataColumn} as data,
          ${createdAtColumn}::text as created_at,
          ${updatedAtColumn}::text as updated_at
        FROM ${tableName}
        ORDER BY ${pathColumn};
      `);
            let count = 0;
            for (const row of result.rows) {
                let data = row.data;
                if (typeof data === 'string') {
                    try {
                        data = JSON.parse(data);
                    }
                    catch { }
                    ;
                }
                this.store(row.path, data, row.created_at || null, row.updated_at || null);
                count++;
            }
            return count;
        }
        finally {
            client.release();
        }
    }
    async exportToPostgres(tableName = 'tree_data', createTable = true, clearExisting = false) {
        const client = await this.pool.connect();
        try {
            await client.query('CREATE EXTENSION IF NOT EXISTS ltree;');
            if (createTable) {
                await client.query(`
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id SERIAL PRIMARY KEY,
            path LTREE UNIQUE NOT NULL,
            data JSONB,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
          );
        `);
                await client.query(`CREATE INDEX IF NOT EXISTS ${tableName}_path_idx ON ${tableName} USING GIST (path);`);
                await client.query(`CREATE INDEX IF NOT EXISTS ${tableName}_data_idx ON ${tableName} USING GIN (data);`);
            }
            if (clearExisting) {
                await client.query(`TRUNCATE TABLE ${tableName}`);
            }
            let count = 0;
            for (const [path, node] of this.data) {
                try {
                    await client.query(`
            INSERT INTO ${tableName} (path, data, created_at, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (path)
            DO UPDATE SET
              data = EXCLUDED.data,
              updated_at = EXCLUDED.updated_at;
          `, [path, JSON.stringify(node.data), node.createdAt, node.updatedAt]);
                    count++;
                }
                catch (e) {
                    console.error(`Error exporting path ${path}:`, e);
                }
            }
            return count;
        }
        finally {
            client.release();
        }
    }
    async syncWithPostgres(direction = 'both') {
        const stats = { imported: 0, exported: 0 };
        if (direction === 'import' || direction === 'both') {
            try {
                stats.imported = await this.importFromPostgres(this.tableName);
            }
            catch (e) {
                console.error('Import failed:', e);
            }
        }
        if (direction === 'export' || direction === 'both') {
            try {
                stats.exported = await this.exportToPostgres(this.tableName);
            }
            catch (e) {
                console.error('Export failed:', e);
            }
        }
        return stats;
    }
    getStats() {
        if (this.data.size === 0) {
            return { total_nodes: 0, max_depth: 0, avg_depth: 0, root_nodes: 0, leaf_nodes: 0 };
        }
        const paths = Array.from(this.data.keys());
        const depths = paths.map(p => this.nlevel(p));
        const rootCount = paths.filter(p => this.nlevel(p) === 1).length;
        let leafCount = 0;
        for (const p of paths) {
            if (!paths.some(o => this.ltreeAncestor(p, o)))
                leafCount++;
        }
        return {
            total_nodes: this.data.size,
            max_depth: Math.max(...depths),
            avg_depth: depths.reduce((s, d) => s + d, 0) / depths.length,
            root_nodes: rootCount,
            leaf_nodes: leafCount
        };
    }
    clear() {
        this.data.clear();
    }
    size() {
        return this.data.size;
    }
    getAllPaths() {
        return Array.from(this.data.keys()).sort();
    }
    deepClone(obj) {
        if (obj === null || typeof obj !== 'object')
            return obj;
        if (obj instanceof Date)
            return new Date(obj.getTime());
        if (Array.isArray(obj))
            return obj.map(i => this.deepClone(i));
        if (obj instanceof Map) {
            const cloned = new Map();
            for (const [k, v] of obj) {
                cloned.set(k, this.deepClone(v));
            }
            return cloned;
        }
        if (obj instanceof Set) {
            const cloned = new Set();
            for (const v of obj) {
                cloned.add(this.deepClone(v));
            }
            return cloned;
        }
        const clonedObj = {};
        for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                clonedObj[key] = this.deepClone(obj[key]);
            }
        }
        return clonedObj;
    }
    async close() {
        await this.pool.end();
    }
}
exports.BasicConstructDB = BasicConstructDB;
