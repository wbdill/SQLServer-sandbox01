-- 
/*
================================================================================
Script:    Generate MERGE DDL with stg_<table> as source and <table> as target.  TRUNCATEs stg table after MERGE
Output:    One row per table
Notes:     In Notepad++ replace dash dash star with \r\n
Tested on: SQL Server 2022
Created:   2025-09-10 - ChatGPT-5 w/ several iterations by bdill
Modified:  2025-09-12 - now truncates stg table and wrapped in TRY/CATCH and TRAN
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
      b.SchemaName
    , 'stg_' + b.TableName AS SourceTable
    , b.TableName AS TargetTable
    , b.SchemaName + '.MERGE_' + b.TableName AS StoredProcName 
    , 'GO ' + CHAR(13) + CHAR(10) + '--*' +
    'CREATE OR ALTER PROCEDURE ' + b.SchemaName + '.MERGE_' + b.TableName + CHAR(13) + CHAR(10) + '--*' +
    'AS ' + CHAR(13) + CHAR(10) + '--*' +
    'BEGIN ' + CHAR(13) + CHAR(10) + '--*' +
    '    BEGIN TRY ' + CHAR(13) + CHAR(10) + '--*' +
    '        BEGIN TRAN;' + CHAR(13) + CHAR(10) + '--*' +
    '        MERGE ' + b.SchemaName + '.' + b.TableName + ' AS T' + CHAR(13) + CHAR(10) + '--*' +
    '        USING ' + b.SchemaName + '.stg_' + b.TableName + ' AS S' + CHAR(13) + CHAR(10) + '--*' +
    '        ON ' + (
            SELECT STRING_AGG('T.' + pk.ColumnName + ' = S.' + pk.ColumnName, ' AND ')
            FROM PKCols pk
            WHERE pk.object_id = b.object_id
        ) + CHAR(13) + CHAR(10) + '--*' +
    '        WHEN MATCHED THEN ' + CHAR(13) + CHAR(10) + '--*' +
    '            UPDATE SET ' + (
            SELECT STRING_AGG('T.' + c.ColumnName + ' = S.' + c.ColumnName, ', ')
            FROM Cols c
            WHERE c.object_id = b.object_id
              AND c.is_identity = 0
              AND c.ColumnName NOT IN (
                    SELECT pk.ColumnName FROM PKCols pk WHERE pk.object_id = b.object_id
              )
        ) + CHAR(13) + CHAR(10) + '--*' +
    '        WHEN NOT MATCHED BY TARGET THEN ' + CHAR(13) + CHAR(10) + '--*' +
    '            INSERT (' + (
            SELECT STRING_AGG(c.ColumnName, ', ')
            FROM Cols c
            WHERE c.object_id = b.object_id
              AND c.is_identity = 0
        ) + ')' + CHAR(13) + CHAR(10) + '--*' +
    '            VALUES (' + (
            SELECT STRING_AGG('S.' + c.ColumnName, ', ')
            FROM Cols c
            WHERE c.object_id = b.object_id
              AND c.is_identity = 0
        ) + ');' + CHAR(13) + CHAR(10) + '--*' +
    '        TRUNCATE TABLE ' + b.SchemaName + '.stg_' + b.TableName + ';' + CHAR(13) + CHAR(10) + '--*' +
    '        COMMIT TRAN;' + CHAR(13) + CHAR(10) + '--*' +
    '    END TRY ' + CHAR(13) + CHAR(10) + '--*' +
    '    BEGIN CATCH ' + CHAR(13) + CHAR(10) + '--*' +
    '        IF @@TRANCOUNT > 0 ROLLBACK TRAN;' + CHAR(13) + CHAR(10) + '--*' +
    '        THROW; ' + CHAR(13) + CHAR(10) + '--*' +
    '    END CATCH ' + CHAR(13) + CHAR(10) + '--*' +
    'END;' AS MergeProcScript
FROM BaseTables b
ORDER BY b.TableName;
