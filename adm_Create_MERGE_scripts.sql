/*
================================================================================
Script: Generate MERGE DDL with stg_<table> as source and <table> as target
Output: One row per table
Tested on: SQL Server 2022
Created: 2025-09-10 - ChatGPT-5 w/ several iterations by bdill
================================================================================
*/
;WITH BaseTables AS (
    SELECT t.object_id, t.name AS TableName, s.name AS SchemaName
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name NOT LIKE 'stg[_]%' -- exclude staging tables
)
, PKCols AS (
    SELECT kc.parent_object_id AS object_id,
           ic.key_ordinal,
           c.name AS ColumnName
    FROM sys.key_constraints kc
    JOIN sys.index_columns ic 
         ON kc.parent_object_id = ic.object_id 
        AND kc.unique_index_id = ic.index_id
    JOIN sys.columns c 
         ON ic.object_id = c.object_id 
        AND ic.column_id = c.column_id
    WHERE kc.type = 'PK'
)
, Cols AS (
    SELECT c.object_id, c.name AS ColumnName, c.is_identity
    FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    WHERE t.name NOT LIKE 'stg[_]%'
)
SELECT 
    b.SchemaName,
    b.TableName AS TargetTable,
    'stg_' + b.TableName AS SourceTable,
    'GO' + CHAR(13) +
    'CREATE OR ALTER PROCEDURE ' + QUOTENAME(b.SchemaName) + '.MERGE_' + QUOTENAME(b.TableName) + CHAR(13) +
    'AS' + CHAR(13) +
    'BEGIN' + CHAR(13) +
    '    MERGE ' + QUOTENAME(b.SchemaName) + '.' + QUOTENAME(b.TableName) + ' AS T' + CHAR(13) +
    '    USING ' + QUOTENAME(b.SchemaName) + '.' + QUOTENAME('stg_' + b.TableName) + ' AS S' + CHAR(13) +
    '    ON ' + STUFF((
            SELECT ' AND T.' + QUOTENAME(pk.ColumnName) + ' = S.' + QUOTENAME(pk.ColumnName)
            FROM PKCols pk
            WHERE pk.object_id = b.object_id
            ORDER BY pk.key_ordinal
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 5, '') + CHAR(13) +
    '    WHEN MATCHED THEN' + CHAR(13) +
    '        UPDATE SET ' + STUFF((
            SELECT ', T.' + QUOTENAME(c.ColumnName) + ' = S.' + QUOTENAME(c.ColumnName)
            FROM Cols c
            WHERE c.object_id = b.object_id
              AND c.is_identity = 0 -- don’t update identity columns
              AND c.ColumnName NOT IN (
                    SELECT pk.ColumnName FROM PKCols pk WHERE pk.object_id = b.object_id
              )
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + CHAR(13) +
    '    WHEN NOT MATCHED BY TARGET THEN' + CHAR(13) +
    '        INSERT (' + STUFF((
            SELECT ', ' + QUOTENAME(c.ColumnName)
            FROM Cols c
            WHERE c.object_id = b.object_id
              AND c.is_identity = 0 -- don’t insert into identity
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')' + CHAR(13) +
    '        VALUES (' + STUFF((
            SELECT ', S.' + QUOTENAME(c.ColumnName)
            FROM Cols c
            WHERE c.object_id = b.object_id
              AND c.is_identity = 0 -- don’t insert into identity
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ');' + CHAR(13) +
    'END;' AS MergeProcScript
FROM BaseTables b
ORDER BY b.TableName;
