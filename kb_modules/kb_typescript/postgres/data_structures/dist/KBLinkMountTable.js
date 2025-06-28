"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KBLinkMountTable = void 0;
class KBLinkMountTable {
    /**
     * @param client  An instance of pg.Client (already connected)
     * @param base    The base name of your table (without "_link_mount" suffix)
     */
    constructor(client, base) {
        this.client = client;
        this.baseTable = `${base}_link_mount`;
    }
    /**
     * Find all rows where link_name = linkName,
     * optionally filtered by knowledge_base.
     */
    async findRecordsByLinkName(linkName, kb) {
        let query;
        let params;
        if (kb == null) {
            query = `SELECT * FROM ${this.baseTable} WHERE link_name = $1`;
            params = [linkName];
        }
        else {
            query = `
        SELECT *
        FROM ${this.baseTable}
        WHERE link_name = $1 AND knowledge_base = $2
      `;
            params = [linkName, kb];
        }
        const res = await this.client.query(query, params);
        return res.rows;
    }
    /**
     * Find all rows where mount_path = mountPath,
     * optionally filtered by knowledge_base.
     */
    async findRecordsByMountPath(mountPath, kb) {
        let query;
        let params;
        if (kb == null) {
            query = `SELECT * FROM ${this.baseTable} WHERE mount_path = $1`;
            params = [mountPath];
        }
        else {
            query = `
        SELECT *
        FROM ${this.baseTable}
        WHERE mount_path = $1 AND knowledge_base = $2
      `;
            params = [mountPath, kb];
        }
        const res = await this.client.query(query, params);
        return res.rows;
    }
    /**
     * Return all distinct link_name values, sorted ascending.
     */
    async findAllLinkNames() {
        const query = `
      SELECT DISTINCT link_name
      FROM ${this.baseTable}
      ORDER BY link_name
    `;
        const res = await this.client.query(query);
        return res.rows.map(r => r.link_name);
    }
    /**
     * Return all distinct mount_path values, sorted ascending.
     */
    async findAllMountPaths() {
        const query = `
      SELECT DISTINCT mount_path
      FROM ${this.baseTable}
      ORDER BY mount_path
    `;
        const res = await this.client.query(query);
        return res.rows.map(r => r.mount_path);
    }
}
exports.KBLinkMountTable = KBLinkMountTable;
