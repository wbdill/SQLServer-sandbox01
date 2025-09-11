/*
================================================================================
Script: Generate CREATE TABLE DDL with "stg_" prefix for tables and constraints
Output: One row per table
Tested on: SQL Server 2022
Created: 2025-09-10 - ChatGPT-5 w/ several iterations by bdill
================================================================================
*/

;WITH TableInfo AS (
    SELECT 
        t.object_id,
        SchemaName = s.name,
        TableName = t.name,
        PrefixedTableName = 'stg_' + t.name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
)
SELECT 
    ti.SchemaName,
    ti.TableName,
    ti.PrefixedTableName,
    DDL =
    'CREATE TABLE [' + ti.SchemaName + '].[' + ti.PrefixedTableName + '] (' + CHAR(13) +
    -- Columns
    STRING_AGG(
        '    [' + c.name + '] ' +
        CASE 
            WHEN ty.name IN ('varchar','char','varbinary','binary','nvarchar','nchar')
                THEN ty.name + '(' + 
                     CASE WHEN c.max_length = -1 THEN 'MAX' 
                          WHEN ty.name IN ('nchar','nvarchar') THEN CAST(c.max_length/2 AS VARCHAR(10)) 
                          ELSE CAST(c.max_length AS VARCHAR(10)) END + ')'
            WHEN ty.name IN ('decimal','numeric')
                THEN ty.name + '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
            ELSE ty.name
        END + 
        CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END
        +
        -- Default constraints inline if present
        ISNULL(' CONSTRAINT [stg_' + dc.name + '] DEFAULT ' + dc.definition, '')
        , ',' + CHAR(13)
    ) WITHIN GROUP (ORDER BY c.column_id)
    -- Primary Key
    + ISNULL((
        SELECT STRING_AGG(definition, ',' + CHAR(13))
        FROM (
            SELECT '    CONSTRAINT [stg_' + kc.name + '] PRIMARY KEY (' +
                   STRING_AGG('[' + col.name + ']', ',') 
                       WITHIN GROUP (ORDER BY ic.key_ordinal) + ')'
                   AS definition
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic 
                ON kc.parent_object_id = ic.object_id 
               AND kc.unique_index_id = ic.index_id
            JOIN sys.columns col 
                ON ic.object_id = col.object_id 
               AND ic.column_id = col.column_id
            WHERE kc.parent_object_id = ti.object_id
              AND kc.type = 'PK'
            GROUP BY kc.name
        ) x
    ), '') 
    -- Unique Constraints
    + ISNULL((
        SELECT STRING_AGG(definition, ',' + CHAR(13))
        FROM (
            SELECT '    CONSTRAINT [stg_' + kc.name + '] UNIQUE (' +
                   STRING_AGG('[' + col.name + ']', ',') 
                       WITHIN GROUP (ORDER BY ic.key_ordinal) + ')'
                   AS definition
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic 
                ON kc.parent_object_id = ic.object_id 
               AND kc.unique_index_id = ic.index_id
            JOIN sys.columns col 
                ON ic.object_id = col.object_id 
               AND ic.column_id = col.column_id
            WHERE kc.parent_object_id = ti.object_id
              AND kc.type = 'UQ'
            GROUP BY kc.name
        ) x
    ), '') 
    -- Check Constraints
    + ISNULL((
        SELECT STRING_AGG('    CONSTRAINT [stg_' + cc.name + '] CHECK ' + cc.definition, ',' + CHAR(13))
        FROM sys.check_constraints cc
        WHERE cc.parent_object_id = ti.object_id
    ), '') 
    + CHAR(13) +
    ');' + CHAR(13) + CHAR(13)
FROM TableInfo ti
JOIN sys.columns c ON ti.object_id = c.object_id
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
GROUP BY ti.SchemaName, ti.TableName, ti.PrefixedTableName, ti.object_id
ORDER BY ti.SchemaName, ti.TableName;
