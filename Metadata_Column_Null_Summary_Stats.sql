-- =======================================================================================
-- Name: Metadata_Column_Null_Summary_Stats.sql
-- Desc: For each column in the DB, shows DistinctCount, # of non-null/non-empty counts (and percents)
-- Note: relies on sp_spaceused which CAN cause issues if a table name is reused in a different schema.
-- Init: 2022-06-14 - Mike McFarren (original author)
-- Upd:  2022-06-14 - bdill - added DataTyoe, NullSpec, TotalRows to #colcounts and populated at the end.

DECLARE @offset INT = 0
DECLARE @limit INT = 50 -- < number of tables at a time.  Can take a long time to run on large tables

DROP TABLE IF EXISTS #tables 
DROP TABLE IF EXISTS #colcounts
DROP TABLE IF EXISTS #spaceUsed 

CREATE TABLE #tables (
	  ObjectId INT
	, SchemaName VARCHAR(50)
	, TableName VARCHAR(100))

CREATE TABLE #colcounts (
	  SchemaName VARCHAR(50)
	, TableName VARCHAR(100)
	, ColumnName VARCHAR(100)
	, DataType VARCHAR(100) NULL
	, NullSpec VARCHAR(50) NULL
	, ColumnId INT
	, DistinctCount INT NULL
	, NonNullCount INT NULL
	, NonNullPercent DECIMAL(4, 3) NULL
	, TotalRows BIGINT NULL)

CREATE TABLE #spaceUsed (
    [name] VARCHAR(255)
  , [rows] INT
  , reserved VARCHAR(50)
  , [data] VARCHAR(50)
  , index_size VARCHAR(50)
  , unused VARCHAR(50));

INSERT INTO #tables ( ObjectId, SchemaName, TableName )
SELECT object_id, OBJECT_SCHEMA_NAME(object_id) AS schemaname, name AS tablename FROM sys.tables WHERE type = 'U' AND name NOT IN ('sysdiagrams') 
ORDER BY OBJECT_SCHEMA_NAME(object_id), name
OFFSET @offset ROWS
FETCH NEXT @limit ROWS ONLY;

-- =======================================================================================
/* declare variables */
DECLARE @objectId INT, @schemaName VARCHAR(50), @tableName VARCHAR(100)

DECLARE tblcur CURSOR FAST_FORWARD READ_ONLY FOR 
	SELECT * FROM #tables

OPEN tblcur

FETCH NEXT FROM tblcur INTO @objectId, @schemaName, @tableName

WHILE @@FETCH_STATUS = 0
BEGIN
	DECLARE @table VARCHAR(100) = @schemaName + '.' + @tableName
	PRINT @table
	
	INSERT INTO #spaceUsed ([name], rows, reserved, data, index_size, unused)
	EXEC sys.sp_spaceused @objname = @table

	DECLARE @sql varchar(MAX)='';

	DECLARE @columns INT = (SELECT COUNT(*) FROM sys.columns WHERE [OBJECT_ID] = @objectId)

	IF (@columns > 0)
	BEGIN
		SELECT @sql=@sql+' select ''' + @schemaName+ ''' AS sname, ''' + @tableName+ ''' AS tname, ''' + name + ''' AS fld, ' + CONVERT(VARCHAR(12), column_id) + ' AS cid, ' + 
			CASE WHEN TYPE_NAME(user_type_id) IN ('ntext','image','varbinary','uniqueidentifier') 
				THEN 'NULL AS DCount, NULL AS NonNullCount' 
				ELSE 'COUNT(DISTINCT [' + name  +']) AS DCount, SUM(CASE WHEN [' + name + '] IS NULL OR CONVERT(VARCHAR, [' + name + ']) = '''' THEN 0 ELSE 1 END) AS NonNullCount' 
				END 
			+ ' FROM  [' + @schemaName + '].[' + @tableName + '] UNION '
		FROM sys.columns 
		WHERE [object_id]=@objectId
		ORDER BY column_id;

		SET @sql = SUBSTRING(@sql, 1 ,LEN(@sql)-6); -- chop off the last 'UNION '

		PRINT @Sql
		INSERT INTO #colcounts ( SchemaName, TableName, ColumnName, ColumnId, DistinctCount, NonNullCount )
		EXEC(@sql)

	END

    FETCH NEXT FROM tblcur INTO @objectId, @schemaName, @tableName
END

CLOSE tblcur
DEALLOCATE tblcur

--SELECT * FROM #spaceUsed

-- =======================================================================================
UPDATE #colcounts 
SET NonNullPercent = CASE WHEN s.rows = 0 THEN 0.0 WHEN d.NonNullCount IS NULL THEN NULL ELSE CONVERT(FLOAT, d.NonNullCount) / CONVERT(FLOAT, s.rows) END
, TotalRows = s.rows
FROM #colcounts d JOIN #spaceUsed s ON d.TableName = s.name

-- show column's datatype
UPDATE #colcounts SET
  DataType = UPPER(X.DataType)
, NullSpec = X.NullSpec
FROM (
	SELECT S.name AS SchemaName, T.name AS TableName, C.name AS ColumnName
		, CASE WHEN Y.name IN ('int', 'bigint', 'tinyint', 'bit', 'date', 'smalldatetime') THEN Y.name
		  WHEN Y.name IN ('char', 'nchar', 'varchar', 'nvarchar') AND y.max_length = 8000 THEN Y.name + '(MAX)' 
		  WHEN Y.name IN ('char', 'nchar', 'varchar', 'nvarchar') AND y.max_length > 0 THEN Y.name + '(' + CONVERT(VARCHAR(50), y.max_length) + ')' 
		  WHEN Y.name IN ('datetime2') THEN Y.name + '(' + CONVERT(varchar(50), C.scale) + ')'
		  ELSE Y.name END AS DataType
		, CASE C.is_nullable WHEN 1 THEN 'NULL' ELSE 'NOT NULL' END AS NullSpec
	FROM sys.schemas AS S
	JOIN sys.tables AS T ON T.schema_id = S.schema_id
	JOIN sys.columns AS C ON C.object_id = T.object_id
	JOIN sys.types AS y ON y.system_type_id = C.system_type_id
) AS X 
WHERE X.SchemaName = #colcounts.SchemaName AND X.TableName = #colcounts.TableName AND X.ColumnName = #colcounts.ColumnName

UPDATE #colcounts SET NonNullCount = NULL, NonNullPercent = NULL WHERE NullSpec = 'NOT NULL' OR TotalRows = 0

--SELECT * FROM #spaceUsed AS SU
SELECT * FROM #colcounts ORDER BY SchemaName, TableName, ColumnId