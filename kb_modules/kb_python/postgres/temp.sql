-- ============================================================================
-- ChainTree Bitfield System - PostgreSQL Schema
-- ============================================================================
-- This creates all tables and functions needed for the bitfield system
-- Run this ONCE to initialize the database
-- ============================================================================

-- Drop existing objects if recreating
-- DROP TABLE IF EXISTS bitfield_definitions CASCADE;
-- DROP TABLE IF EXISTS bitfield_registry CASCADE;

-- ============================================================================
-- Table: bitfield_registry
-- Tracks all tables that use bitfields
-- ============================================================================
CREATE TABLE IF NOT EXISTS bitfield_registry (
    table_name TEXT PRIMARY KEY,
    field_name TEXT NOT NULL UNIQUE,
    flags_column TEXT NOT NULL DEFAULT 'status_flags',
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CHECK (table_name ~ '^[a-zA-Z][a-zA-Z0-9_]*$'),  -- Valid identifier
    CHECK (field_name ~ '^[a-zA-Z][a-zA-Z0-9_]*$'),
    CHECK (flags_column ~ '^[a-zA-Z][a-zA-Z0-9_]*$')
);

COMMENT ON TABLE bitfield_registry IS 'Registry of all tables using bitfield columns';
COMMENT ON COLUMN bitfield_registry.table_name IS 'Name of the table with bitfield column';
COMMENT ON COLUMN bitfield_registry.field_name IS 'Internal field name (sanitized from table_name)';
COMMENT ON COLUMN bitfield_registry.flags_column IS 'Name of the BIGINT column holding flags';

-- ============================================================================
-- Table: bitfield_definitions
-- Stores flag names and bit positions for each field
-- ============================================================================
CREATE TABLE IF NOT EXISTS bitfield_definitions (
    field_name TEXT NOT NULL,
    flag_name TEXT NOT NULL,
    bit_position INTEGER NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    PRIMARY KEY (field_name, flag_name),
    CHECK (bit_position >= 0 AND bit_position < 64),
    CHECK (flag_name ~ '^[A-Z][A-Z0-9_]*$'),  -- Uppercase with underscores
    
    -- Foreign key to registry
    FOREIGN KEY (field_name) REFERENCES bitfield_registry(field_name)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Unique constraint: one flag per bit position per field
CREATE UNIQUE INDEX IF NOT EXISTS idx_bitfield_definitions_unique_bit 
ON bitfield_definitions(field_name, bit_position);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_bitfield_definitions_field 
ON bitfield_definitions(field_name);

COMMENT ON TABLE bitfield_definitions IS 'Defines flag names and bit positions for bitfield columns';
COMMENT ON COLUMN bitfield_definitions.field_name IS 'References bitfield_registry.field_name';
COMMENT ON COLUMN bitfield_definitions.flag_name IS 'Name of the flag (e.g., ACTIVE, ENABLED)';
COMMENT ON COLUMN bitfield_definitions.bit_position IS 'Bit position (0-63) in the BIGINT field';

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Function to get bit mask for a flag
CREATE OR REPLACE FUNCTION bitfield_get_mask(field TEXT, flag TEXT)
RETURNS BIGINT AS $$
    SELECT (1::BIGINT << bit_position)
    FROM bitfield_definitions
    WHERE field_name = field AND flag_name = flag;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

COMMENT ON FUNCTION bitfield_get_mask IS 'Get the bit mask for a named flag';

-- Function to get all flags as text
CREATE OR REPLACE FUNCTION bitfield_get_flag_names(field TEXT)
RETURNS TEXT[] AS $$
    SELECT array_agg(flag_name ORDER BY bit_position)
    FROM bitfield_definitions
    WHERE field_name = field;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION bitfield_get_flag_names IS 'Get array of all flag names for a field';

-- Function to validate a bit position is available
CREATE OR REPLACE FUNCTION bitfield_bit_position_available(field TEXT, bit_pos INTEGER)
RETURNS BOOLEAN AS $$
    SELECT NOT EXISTS (
        SELECT 1 FROM bitfield_definitions
        WHERE field_name = field AND bit_position = bit_pos
    );
$$ LANGUAGE SQL STABLE;

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bitfield_registry_updated_at
BEFORE UPDATE ON bitfield_registry
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER bitfield_definitions_updated_at
BEFORE UPDATE ON bitfield_definitions
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- View: bitfield_summary
-- Summary view of all bitfield tables and their flags
-- ============================================================================
CREATE OR REPLACE VIEW bitfield_summary AS
SELECT 
    r.table_name,
    r.field_name,
    r.flags_column,
    r.description as table_description,
    COUNT(d.flag_name) as flag_count,
    array_agg(d.flag_name ORDER BY d.bit_position) as flag_names,
    r.created_at,
    r.updated_at,
    -- Check if table actually exists
    EXISTS (
        SELECT 1 FROM information_schema.tables t
        WHERE t.table_schema = 'public' 
        AND t.table_name = r.table_name
    ) as table_exists
FROM bitfield_registry r
LEFT JOIN bitfield_definitions d ON r.field_name = d.field_name
GROUP BY r.table_name, r.field_name, r.flags_column, r.description, r.created_at, r.updated_at
ORDER BY r.table_name;

COMMENT ON VIEW bitfield_summary IS 'Summary of all bitfield-enabled tables with flag counts';

-- ============================================================================
-- View: bitfield_details
-- Detailed view of all flags across all fields
-- ============================================================================
CREATE OR REPLACE VIEW bitfield_details AS
SELECT 
    r.table_name,
    r.field_name,
    r.flags_column,
    d.flag_name,
    d.bit_position,
    (1::BIGINT << d.bit_position) as bit_mask,
    '0x' || lpad(to_hex(1::BIGINT << d.bit_position), 16, '0') as hex_mask,
    d.description as flag_description,
    r.description as table_description
FROM bitfield_registry r
JOIN bitfield_definitions d ON r.field_name = d.field_name
ORDER BY r.table_name, d.bit_position;

COMMENT ON VIEW bitfield_details IS 'Detailed view of all flags with masks and descriptions';

-- ============================================================================
-- Utility Functions for Querying
-- ============================================================================

-- Function to list all bitfield tables
CREATE OR REPLACE FUNCTION list_bitfield_tables()
RETURNS TABLE(
    table_name TEXT,
    field_name TEXT,
    flags_column TEXT,
    flag_count BIGINT,
    table_exists BOOLEAN
) AS $$
    SELECT 
        table_name,
        field_name,
        flags_column,
        flag_count,
        table_exists
    FROM bitfield_summary
    ORDER BY table_name;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION list_bitfield_tables IS 'List all registered bitfield tables';

-- Function to get flags for a specific table
CREATE OR REPLACE FUNCTION get_table_flags(p_table_name TEXT)
RETURNS TABLE(
    flag_name TEXT,
    bit_position INTEGER,
    bit_mask BIGINT,
    description TEXT
) AS $$
    SELECT 
        d.flag_name,
        d.bit_position,
        (1::BIGINT << d.bit_position) as bit_mask,
        d.description
    FROM bitfield_registry r
    JOIN bitfield_definitions d ON r.field_name = d.field_name
    WHERE r.table_name = p_table_name
    ORDER BY d.bit_position;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION get_table_flags IS 'Get all flags for a specific table';

-- Function to find orphaned registrations
CREATE OR REPLACE FUNCTION find_orphaned_bitfield_registrations()
RETURNS TABLE(table_name TEXT) AS $$
    SELECT r.table_name
    FROM bitfield_registry r
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.tables t
        WHERE t.table_schema = 'public' 
        AND t.table_name = r.table_name
    );
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION find_orphaned_bitfield_registrations IS 'Find bitfield registrations for non-existent tables';

-- Function to find unregistered BIGINT columns
CREATE OR REPLACE FUNCTION find_unregistered_bigint_columns()
RETURNS TABLE(table_name TEXT, column_name TEXT) AS $$
    SELECT c.table_name::TEXT, c.column_name::TEXT
    FROM information_schema.columns c
    LEFT JOIN bitfield_registry r 
        ON c.table_name = r.table_name 
        AND c.column_name = r.flags_column
    WHERE c.table_schema = 'public'
      AND c.data_type = 'bigint'
      AND r.table_name IS NULL
    ORDER BY c.table_name, c.column_name;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION find_unregistered_bigint_columns IS 'Find BIGINT columns that might be bitfields but are not registered';

-- ============================================================================
-- Validation Function
-- ============================================================================
CREATE OR REPLACE FUNCTION validate_bitfield_system()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Check 1: Registry table exists
    RETURN QUERY
    SELECT 
        'Registry Table'::TEXT,
        'OK'::TEXT,
        'bitfield_registry table exists'::TEXT;
    
    -- Check 2: Definitions table exists
    RETURN QUERY
    SELECT 
        'Definitions Table'::TEXT,
        'OK'::TEXT,
        'bitfield_definitions table exists'::TEXT;
    
    -- Check 3: Count registrations
    RETURN QUERY
    SELECT 
        'Registered Tables'::TEXT,
        'INFO'::TEXT,
        COUNT(*)::TEXT || ' tables registered'
    FROM bitfield_registry;
    
    -- Check 4: Count definitions
    RETURN QUERY
    SELECT 
        'Total Flags'::TEXT,
        'INFO'::TEXT,
        COUNT(*)::TEXT || ' flags defined'
    FROM bitfield_definitions;
    
    -- Check 5: Orphaned registrations
    RETURN QUERY
    SELECT 
        'Orphaned Registrations'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK'::TEXT ELSE 'WARNING'::TEXT END,
        COUNT(*)::TEXT || ' registrations without tables'
    FROM (SELECT * FROM find_orphaned_bitfield_registrations()) orphaned;
    
    -- Check 6: Duplicate bit positions
    RETURN QUERY
    SELECT 
        'Duplicate Bit Positions'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK'::TEXT ELSE 'ERROR'::TEXT END,
        COUNT(*)::TEXT || ' duplicate bit positions found'
    FROM (
        SELECT field_name, bit_position
        FROM bitfield_definitions
        GROUP BY field_name, bit_position
        HAVING COUNT(*) > 1
    ) dups;
    
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION validate_bitfield_system IS 'Validate the integrity of the bitfield system';

-- ============================================================================
-- Example Usage Queries
-- ============================================================================

-- List all bitfield tables
-- SELECT * FROM list_bitfield_tables();

-- Get flags for a specific table
-- SELECT * FROM get_table_flags('motor_nodes');

-- View summary
-- SELECT * FROM bitfield_summary;

-- View details
-- SELECT * FROM bitfield_details WHERE table_name = 'motor_nodes';

-- Validate system
-- SELECT * FROM validate_bitfield_system();

-- Find orphaned registrations
-- SELECT * FROM find_orphaned_bitfield_registrations();

-- Find unregistered BIGINT columns
-- SELECT * FROM find_unregistered_bigint_columns();

-- ============================================================================
-- Grant permissions (adjust as needed for your security model)
-- ============================================================================

-- Example: Grant to a specific role
-- GRANT SELECT, INSERT, UPDATE, DELETE ON bitfield_registry TO chaintree_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON bitfield_definitions TO chaintree_user;
-- GRANT SELECT ON bitfield_summary TO chaintree_user;
-- GRANT SELECT ON bitfield_details TO chaintree_user;
-- GRANT EXECUTE ON FUNCTION list_bitfield_tables() TO chaintree_user;
-- GRANT EXECUTE ON FUNCTION get_table_flags(TEXT) TO chaintree_user;

-- ============================================================================
-- Initialization Complete
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Bitfield System Initialized';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - bitfield_registry';
    RAISE NOTICE '  - bitfield_definitions';
    RAISE NOTICE 'Views created:';
    RAISE NOTICE '  - bitfield_summary';
    RAISE NOTICE '  - bitfield_details';
    RAISE NOTICE 'Functions created:';
    RAISE NOTICE '  - list_bitfield_tables()';
    RAISE NOTICE '  - get_table_flags(table_name)';
    RAISE NOTICE '  - find_orphaned_bitfield_registrations()';
    RAISE NOTICE '  - find_unregistered_bigint_columns()';
    RAISE NOTICE '  - validate_bitfield_system()';
    RAISE NOTICE '========================================';
END $$;

