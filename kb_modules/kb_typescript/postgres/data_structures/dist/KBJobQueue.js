"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KBJobQueue = void 0;
class KBJobQueue {
    constructor(kbSearch, database) {
        this.kbSearch = kbSearch;
        this.client = kbSearch.getConn();
        this.baseTable = `${database}_job`;
    }
    async executeQuery(query, params = []) {
        const res = await this.client.query(query, params);
        return res.rows;
    }
    async executeSingle(query, params = []) {
        const res = await this.client.query(query, params);
        return res.rows[0] ?? null;
    }
    async findJobId(kb, nodeName, properties, nodePath) {
        const results = await this.findJobIds(kb, nodeName, properties, nodePath);
        if (results.length === 0) {
            throw new Error(`No job found matching parameters: name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`);
        }
        if (results.length > 1) {
            throw new Error(`Multiple jobs (${results.length}) found matching parameters: name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`);
        }
        return results[0];
    }
    async findJobIds(kb, nodeName, properties, nodePath) {
        try {
            this.kbSearch.clearFilters();
            this.kbSearch.searchLabel('KB_JOB_QUEUE');
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
            const rows = await this.kbSearch.executeQuery();
            if (!rows.length) {
                throw new Error(`No jobs found matching parameters: name=${nodeName}, properties=${JSON.stringify(properties)}, path=${nodePath}`);
            }
            return rows;
        }
        catch (e) {
            if (e.message.startsWith('No jobs'))
                throw e;
            throw new Error(`Error finding job IDs: ${e.message}`);
        }
    }
    findJobPaths(rows) {
        return rows
            .map(r => r.path)
            .filter((p) => p != null);
    }
    async getQueuedNumber(path) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        const query = `
      SELECT COUNT(*) AS count
      FROM ${this.baseTable}
      WHERE path = $1 AND valid = TRUE
    `;
        const row = await this.executeSingle(query, [path]);
        return parseInt(row?.count ?? '0', 10);
    }
    async getFreeNumber(path) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        const query = `
      SELECT COUNT(*) AS count
      FROM ${this.baseTable}
      WHERE path = $1 AND valid = FALSE
    `;
        const row = await this.executeSingle(query, [path]);
        return parseInt(row?.count ?? '0', 10);
    }
    async peakJobData(path, maxRetries = 3, retryDelay = 1000) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        let attempt = 0;
        while (attempt < maxRetries) {
            try {
                await this.client.query('BEGIN');
                const findQ = `
          SELECT id, data, schedule_at
          FROM ${this.baseTable}
          WHERE path = $1
            AND valid = TRUE
            AND is_active = FALSE
            AND (schedule_at IS NULL OR schedule_at <= NOW())
          ORDER BY schedule_at ASC NULLS FIRST
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        `;
                const job = await this.executeSingle(findQ, [path]);
                if (!job) {
                    await this.client.query('ROLLBACK');
                    return null;
                }
                const updateQ = `
          UPDATE ${this.baseTable}
          SET started_at = NOW(), is_active = TRUE
          WHERE id = $1 AND valid = TRUE AND is_active = FALSE
          RETURNING started_at
        `;
                const upd = await this.executeSingle(updateQ, [job.id]);
                if (!upd) {
                    await this.client.query('ROLLBACK');
                    attempt++;
                    await new Promise(r => setTimeout(r, retryDelay));
                    continue;
                }
                await this.client.query('COMMIT');
                return { ...job, started_at: upd.started_at };
            }
            catch (err) {
                await this.client.query('ROLLBACK');
                attempt++;
                await new Promise(r => setTimeout(r, retryDelay));
            }
        }
        throw new Error(`Could not lock and claim a job for path='${path}' after ${maxRetries} retries`);
    }
    async markJobCompleted(jobId, maxRetries = 3, retryDelay = 1000) {
        if (!Number.isInteger(jobId))
            throw new Error('jobId must be a valid integer');
        let attempt = 0;
        while (attempt < maxRetries) {
            try {
                await this.client.query('BEGIN');
                const lockQ = `SELECT id FROM ${this.baseTable} WHERE id = $1 FOR UPDATE NOWAIT`;
                const row = await this.executeSingle(lockQ, [jobId]);
                if (!row) {
                    await this.client.query('ROLLBACK');
                    throw new Error(`No job found with id=${jobId}`);
                }
                const updQ = `
          UPDATE ${this.baseTable}
          SET completed_at = NOW(), valid = FALSE, is_active = FALSE
          WHERE id = $1
          RETURNING id, completed_at
        `;
                const res = await this.executeSingle(updQ, [jobId]);
                if (!res) {
                    await this.client.query('ROLLBACK');
                    throw new Error(`Failed to mark job ${jobId} as completed`);
                }
                await this.client.query('COMMIT');
                return { success: true, job_id: res.id, completed_at: res.completed_at };
            }
            catch (err) {
                await this.client.query('ROLLBACK');
                attempt++;
                await new Promise(r => setTimeout(r, retryDelay));
            }
        }
        throw new Error(`Could not lock job id=${jobId} after ${maxRetries} attempts`);
    }
    async pushJobData(path, data, maxRetries = 3, retryDelay = 1000) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        if (typeof data !== 'object')
            throw new Error('Data must be a dictionary');
        const selectQ = `
      SELECT id FROM ${this.baseTable}
      WHERE path = $1 AND valid = FALSE
      ORDER BY completed_at ASC
      LIMIT 1 FOR UPDATE SKIP LOCKED
    `;
        const updateQ = `
      UPDATE ${this.baseTable}
      SET data = $1,
          schedule_at = NOW(),
          started_at  = NOW(),
          completed_at= NOW(),
          valid      = TRUE,
          is_active  = FALSE
      WHERE id = $2
      RETURNING id, schedule_at, data
    `;
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const row = await this.executeSingle(selectQ, [path]);
                if (!row) {
                    this.client.query('ROLLBACK');
                    throw new Error(`No available job slot for path '${path}'`);
                }
                const res = await this.executeSingle(updateQ, [JSON.stringify(data), row.id]);
                if (!res) {
                    this.client.query('ROLLBACK');
                    throw new Error(`Failed to update job slot for path '${path}'`);
                }
                this.client.query('COMMIT');
                return { job_id: res.id, schedule_at: res.schedule_at, data: res.data };
            }
            catch (err) {
                this.client.query('ROLLBACK');
                if (attempt < maxRetries) {
                    await new Promise(r => setTimeout(r, retryDelay));
                    continue;
                }
                throw new Error(`Could not acquire lock for path '${path}' after ${maxRetries} attempts: ${err.message}`);
            }
        }
    }
    async listPendingJobs(path, limit, offset = 0) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        let query = `
      SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
      FROM ${this.baseTable}
      WHERE path = $1 AND valid = TRUE AND is_active = FALSE
      ORDER BY schedule_at ASC
    `;
        const params = [path];
        if (limit) {
            params.push(limit);
            query += ` LIMIT $${params.length}`;
        }
        if (offset) {
            params.push(offset);
            query += ` OFFSET $${params.length}`;
        }
        return this.executeQuery(query, params);
    }
    async listActiveJobs(path, limit, offset = 0) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        let query = `
      SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
      FROM ${this.baseTable}
      WHERE path = $1 AND valid = TRUE AND is_active = TRUE
      ORDER BY started_at ASC
    `;
        const params = [path];
        if (limit) {
            params.push(limit);
            query += ` LIMIT $${params.length}`;
        }
        if (offset) {
            params.push(offset);
            query += ` OFFSET $${params.length}`;
        }
        return this.executeQuery(query, params);
    }
    async clearJobQueue(path) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        const updateQ = `
      UPDATE ${this.baseTable}
      SET schedule_at = NOW(),
          started_at  = NOW(),
          completed_at= NOW(),
          is_active   = FALSE,
          valid       = FALSE,
          data        = $1
      WHERE path = $2
      RETURNING id, completed_at
    `;
        try {
            // start transaction
            await this.client.query('BEGIN');
            // now we're in a tx block, so LOCK TABLE is allowed
            await this.client.query(`LOCK TABLE ${this.baseTable} IN EXCLUSIVE MODE`);
            // perform the mass-update
            const rows = await this.executeQuery(updateQ, [
                JSON.stringify({}),
                path,
            ]);
            // commit it all
            await this.client.query('COMMIT');
            return {
                success: true,
                cleared_count: rows.length,
                cleared_jobs: rows,
            };
        }
        catch (err) {
            // if anything goes wrong, roll back
            await this.client.query('ROLLBACK');
            throw err;
        }
    }
    async getJobStatistics(path) {
        if (!path)
            throw new Error('Path cannot be empty or None');
        const statsQ = `
      SELECT
        COUNT(*) AS total_jobs,
        COUNT(*) FILTER (WHERE valid AND NOT is_active) AS pending_jobs,
        COUNT(*) FILTER (WHERE valid AND is_active) AS active_jobs,
        COUNT(*) FILTER (WHERE NOT valid) AS completed_jobs,
        MIN(schedule_at) AS earliest_scheduled,
        MAX(completed_at) AS latest_completed,
        AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) AS avg_processing_time_seconds
      FROM ${this.baseTable}
      WHERE path = $1
    `;
        const row = await this.executeSingle(statsQ, [path]);
        return row || {
            total_jobs: 0,
            pending_jobs: 0,
            active_jobs: 0,
            completed_jobs: 0,
            earliest_scheduled: null,
            latest_completed: null,
            avg_processing_time_seconds: null
        };
    }
    async getJobById(jobId) {
        if (!Number.isInteger(jobId))
            throw new Error('jobId must be a valid integer');
        const query = `
      SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
      FROM ${this.baseTable}
      WHERE id = $1
    `;
        return this.executeSingle(query, [jobId]);
    }
    close() {
        // No-op: client remains managed by KBSearch
    }
}
exports.KBJobQueue = KBJobQueue;
