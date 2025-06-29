"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConstructMemDB = void 0;
const BasicConstructDb_1 = require("./BasicConstructDb");
class ConstructMemDB extends BasicConstructDb_1.BasicConstructDB {
    constructor(host, port, dbname, user, password, tableName) {
        super(host, port, dbname, user, password, tableName);
        this.workingKb = null;
        this.compositePath = new Map();
        this.compositePathValues = new Map();
    }
    /**
     * Create a new knowledge base.
     */
    addKb(kbName, description = '') {
        if (this.compositePath.has(kbName)) {
            throw new Error(`Knowledge base "${kbName}" already exists.`);
        }
        this.compositePath.set(kbName, [kbName]);
        this.compositePathValues.set(kbName, new Map());
        super.addKb(kbName, description);
    }
    /**
     * Select which knowledge base to operate on.
     */
    selectKb(kbName) {
        if (!this.compositePath.has(kbName)) {
            throw new Error(`Knowledge base "${kbName}" does not exist.`);
        }
        this.workingKb = kbName;
    }
    /**
     * Pushes a header node onto the current path and stores it.
     */
    addHeaderNode(link, nodeName, nodeData, description = '') {
        if (!this.workingKb) {
            throw new Error('No knowledge base selected.');
        }
        if (typeof description !== 'string') {
            throw new TypeError('description must be a string');
        }
        if (typeof nodeData !== 'object' || nodeData === null || Array.isArray(nodeData)) {
            throw new TypeError('nodeData must be a non-array object');
        }
        // Annotate nodeData
        if (description) {
            nodeData.description = description;
        }
        const pathStack = this.compositePath.get(this.workingKb);
        pathStack.push(link, nodeName);
        const fullPath = pathStack.join('.');
        const values = this.compositePathValues.get(this.workingKb);
        if (values.has(fullPath)) {
            throw new Error(`Path "${fullPath}" already exists in knowledge base.`);
        }
        values.set(fullPath, true);
        super.store(fullPath, nodeData);
    }
    /**
     * Temporarily pushes a header and immediately pops it back off.
     */
    addInfoNode(link, nodeName, nodeData, description = '') {
        this.addHeaderNode(link, nodeName, nodeData, description);
        // roll back the path so only the header remains
        const stack = this.compositePath.get(this.workingKb);
        stack.pop(); // nodeName
        stack.pop(); // link
    }
    /**
     * Pop a header node, validating that it matches the expected label/name.
     */
    leaveHeaderNode(label, name) {
        if (!this.workingKb) {
            throw new Error('No knowledge base selected.');
        }
        const stack = this.compositePath.get(this.workingKb);
        if (stack.length < 3) {
            throw new Error('Cannot leave header node: insufficient path elements.');
        }
        const poppedName = stack.pop();
        const poppedLabel = stack.pop();
        const errors = [];
        if (poppedLabel !== label) {
            errors.push(`Expected label "${label}", but got "${poppedLabel}"`);
        }
        if (poppedName !== name) {
            errors.push(`Expected name "${name}", but got "${poppedName}"`);
        }
        if (errors.length) {
            // push them back so state is unchanged
            stack.push(poppedLabel, poppedName);
            throw new Error(errors.join('; '));
        }
    }
    /**
     * Ensure every KB’s path is back to its root. Throws if anything
     * was left open.
     */
    checkInstallation() {
        for (const [kb, path] of this.compositePath.entries()) {
            if (path.length !== 1 || path[0] !== kb) {
                throw new Error(`Installation check failed for "${kb}". Current path: ${path.join('.')}`);
            }
        }
        return true;
    }
}
exports.ConstructMemDB = ConstructMemDB;
