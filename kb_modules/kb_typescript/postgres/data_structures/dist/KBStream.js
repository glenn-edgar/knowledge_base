"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KBStream = void 0;
class KBStream {
    constructor(kbSearch, database) {
        this.kbSearch = kbSearch;
        this.client = kbSearch.getConn();
        this.baseTable = `${database}_stream`;
    }
    async executeQuery(query, params = []) {
        const res = await this.client.query(query, params);
        return res.rows;
    }
    async executeSingle(query, params = []) {
        const res = await this.client.query(query, params);
        return res.rows[0] ?? null;
    }
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    async findStreamId(kb, nodeName, properties, nodePath) {
        const results = await this.findStreamIds(kb, nodeName, properties, nodePath);
        if (results.length === 0) {
            throw new Error(`No stream node found matching parameters: name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`);
        }
        if (results.length > 1) {
            throw new Error(`Multiple stream nodes (${results.length}) found matching parameters: name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`);
        }
        return results[0];
    }
    async findStreamIds(kb, nodeName, properties, nodePath) {
        try {
            this.kbSearch.clearFilters();
            this.kbSearch.searchLabel('KB_STREAM_FIELD');
            if (kb)
                this.kbSearch.searchKb(kb);
            if (nodeName)
                this.kbSearch.searchName(nodeName);
            if (properties) {
                for (const [k, v] of Object.entries(properties)) {
                    this.kbSearch.searchPropertyValue(k, v);
                }
            }
            if (nodePath)
                this.kbSearch.searchPath(nodePath);
            const nodeIds = await this.kbSearch.executeQuery();
            if (!nodeIds || nodeIds.length === 0) {
                throw new Error(`No stream nodes found matching parameters: name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`);
            }
            return nodeIds;
        }
        catch (e) {
            if (e.message.startsWith('No stream'))
                throw e;
            throw new Error(`Error finding stream node IDs: ${e.message}`);
        }
    }
    findStreamTableKeys(rows) {
        if (!rows || rows.length === 0)
            return [];
        return rows.map(r => r.path).filter(p => p != null);
    }
    async pushStreamData(path, data, maxRetries = 3, retryDelay = 1000) {
        if (!path)
            throw new Error('Path cannot be empty');
        if (typeof data !== 'object')
            throw new Error('Data must be an object');
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const countRow = await this.executeSingle(`SELECT COUNT(*) AS count FROM ${this.baseTable} WHERE path = $1`, [path]);
                const total = parseInt(countRow?.count ?? '0', 10);
                if (total === 0) {
                    throw new Error(`No records found for path='${path}'. Must pre-allocate.`);
                }
                await this.client.query('BEGIN');
                const row = await this.executeSingle(`
            SELECT id, recorded_at, valid
            FROM ${this.baseTable}
            WHERE path = $1
            ORDER BY recorded_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
          `, [path]);
                if (!row) {
                    await this.client.query('ROLLBACK');
                    if (attempt < maxRetries) {
                        await this.sleep(retryDelay);
                        continue;
                    }
                    else {
                        throw new Error(`Could not lock any row for path='${path}' after ${maxRetries} attempts`);
                    }
                }
                const prevRecordedAt = row.recorded_at;
                const wasValid = row.valid;
                const upd = await this.executeSingle(`
            UPDATE ${this.baseTable}
            SET data = $1, recorded_at = NOW(), valid = TRUE
            WHERE id = $2
            RETURNING id, path, recorded_at, data, valid
          `, [JSON.stringify(data), row.id]);
                if (!upd) {
                    await this.client.query('ROLLBACK');
                    throw new Error(`Failed to update record id=${row.id}`);
                }
                await this.client.query('COMMIT');
                return {
                    id: upd.id,
                    path: upd.path,
                    recorded_at: upd.recorded_at,
                    data: upd.data,
                    valid: upd.valid,
                    previous_recorded_at: prevRecordedAt,
                    was_previously_valid: wasValid,
                    operation: 'circular_buffer_replace'
                };
            }
            catch (e) {
                try {
                    await this.client.query('ROLLBACK');
                }
                catch { }
                if (e.message.startsWith('No records') || e.message.includes('Could not lock')) {
                    throw e;
                }
                if (attempt < maxRetries) {
                    await this.sleep(retryDelay);
                    continue;
                }
                else {
                    throw new Error(`Error pushing stream data: ${e.message}`);
                }
            }
        }
        throw new Error('Unexpected error in pushStreamData');
    }
    async getLatestStreamData(path) {
        if (!path)
            throw new Error('Path cannot be empty');
        const row = await this.executeSingle(`
        SELECT id, path, recorded_at, data, valid
        FROM ${this.baseTable}
        WHERE path = $1 AND valid = TRUE
        ORDER BY recorded_at DESC
        LIMIT 1
      `, [path]);
        return row;
    }
    async getStreamDataCount(path, includeInvalid = false) {
        if (!path)
            throw new Error('Path cannot be empty');
        const query = includeInvalid
            ? `SELECT COUNT(*) AS count FROM ${this.baseTable} WHERE path = $1`
            : `SELECT COUNT(*) AS count FROM ${this.baseTable} WHERE path = $1 AND valid = TRUE`;
        const row = await this.executeSingle(query, [path]);
        return parseInt(row?.count ?? '0', 10);
    }
    async clearStreamData(path, olderThan) {
        if (!path)
            throw new Error('Path cannot be empty');
        try {
            let q, params;
            if (olderThan) {
                q = `
          UPDATE ${this.baseTable}
          SET valid = FALSE
          WHERE path = $1 AND recorded_at < $2 AND valid = TRUE
          RETURNING id, recorded_at
        `;
                params = [path, olderThan];
            }
            else {
                q = `
          UPDATE ${this.baseTable}
          SET valid = FALSE
          WHERE path = $1 AND valid = TRUE
          RETURNING id, recorded_at
        `;
                params = [path];
            }
            const recs = await this.executeQuery(q, params);
            return { success: true, clearedCount: recs.length, clearedRecords: recs };
        }
        catch (e) {
            return { success: false, clearedCount: 0, error: e.message };
        }
    }
    async listStreamData(path, limit, offset = 0, recordedAfter, recordedBefore, order = 'ASC') {
        if (!path)
            throw new Error('Path cannot be empty');
        if (!['ASC', 'DESC'].includes(order))
            throw new Error("Order must be 'ASC' or 'DESC'");
        let query = `
      SELECT id, path, recorded_at, data, valid
      FROM ${this.baseTable}
      WHERE path = $1 AND valid = TRUE
    `;
        const params = [path];
        if (recordedAfter) {
            params.push(recordedAfter);
            query += ` AND recorded_at >= $${params.length}`;
        }
        if (recordedBefore) {
            params.push(recordedBefore);
            query += ` AND recorded_at <= $${params.length}`;
        }
        query += ` ORDER BY recorded_at ${order}`;
        if (limit != null && limit > 0) {
            params.push(limit);
            query += ` LIMIT $${params.length}`;
        }
        if (offset > 0) {
            params.push(offset);
            query += ` OFFSET $${params.length}`;
        }
        return this.executeQuery(query, params);
    }
    async getStreamDataRange(path, startTime, endTime) {
        if (!path)
            throw new Error('Path cannot be empty');
        if (startTime >= endTime)
            throw new Error('startTime must be before endTime');
        const query = `
      SELECT id, path, recorded_at, data, valid
      FROM ${this.baseTable}
      WHERE path = $1
        AND recorded_at >= $2
        AND recorded_at <= $3
        AND valid = TRUE
      ORDER BY recorded_at ASC
    `;
        return this.executeQuery(query, [path, startTime, endTime]);
    }
    async getStreamStatistics(path, includeInvalid = false) {
        if (!path)
            throw new Error('Path cannot be empty');
        const statsQuery = includeInvalid
            ? `
        SELECT 
          COUNT(*) AS total_records,
          COUNT(CASE WHEN valid THEN 1 END) AS valid_records,
          COUNT(CASE WHEN NOT valid THEN 1 END) AS invalid_records,
          MIN(CASE WHEN valid THEN recorded_at END) AS earliest_valid_recorded,
          MAX(CASE WHEN valid THEN recorded_at END) AS latest_valid_recorded,
          MIN(recorded_at) AS earliest_recorded_overall,
          MAX(recorded_at) AS latest_recorded_overall,
          AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) AS avg_interval_seconds_all,
          AVG(CASE WHEN valid THEN EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at))) END) AS avg_interval_seconds_valid
        FROM ${this.baseTable}
        WHERE path = $1
      `
            : `
        SELECT 
          COUNT(*) AS valid_records,
          MIN(recorded_at) AS earliest_recorded,
          MAX(recorded_at) AS latest_recorded,
          AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) AS avg_interval_seconds
        FROM ${this.baseTable}
        WHERE path = $1 AND valid = TRUE
      `;
        const row = await this.executeSingle(statsQuery, [path]);
        return row || {};
    }
    async getStreamDataById(recordId) {
        if (!Number.isInteger(recordId)) {
            throw new Error('recordId must be an integer');
        }
        const query = `
      SELECT id, path, recorded_at, data
      FROM ${this.baseTable}
      WHERE id = $1
    `;
        return this.executeSingle(query, [recordId]);
    }
}
exports.KBStream = KBStream;
