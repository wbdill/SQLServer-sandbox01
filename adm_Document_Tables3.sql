CREATE OR ALTER PROCEDURE dbo.adm_Document_Tables3
	-- =======================================================================================
	-- Desc:	Selects meta data about tables/columns including the MS_Description extended propertes (if any) in each table/column
	-- License:	ShoutOutWare - give me a shout out on Twitter @bdill if this script helped you. :) (retain comments and do not redistribute)
	-- Auth: 	Brian Dill 2021-03-16
	-- Script home: https://github.com/wbdill/SQLServer-sandbox01/blob/master/adm_Document_Tables3.sql
	-- Other useful scripts: https://github.com/wbdill/SQLServer-sandbox01
	-- Upd:     2022-06-07 bdill - added param @ReportType to get tables(only), columns(only) or combined(old style)
	-- Upd:		2022-06-15 bdill - minor syntax changes

	  @TableNameLike VARCHAR(100) = '%'
	, @ColumnNameLike VARCHAR(100) = '%'
	, @ReportType VARCHAR(100) = 'combined'  --  other options: 'tables' or 'columns'

AS
BEGIN 
	SET NOCOUNT ON

	-- =======================================================================================
	-- Create a temp table to hold all of the descriptions for alter joining.

	DROP TABLE IF EXISTS #tmpDescColumns 

	CREATE TABLE #tmpDescColumns (  
		  DescID               INT IDENTITY(1,1) NOT NULL
		, SchemaName           VARCHAR(255) NULL
		, TableName            VARCHAR(255)  NULL
		, ColumnName           VARCHAR(255)  NULL
		, Description          VARCHAR(4000) NULL
	)

	-- =======================================================================================
	-- temp table for all PKs FKs
	
	DROP TABLE IF EXISTS #tmpKeys 

	CREATE TABLE #tmpKeys (
		  SchemaName VARCHAR(100) NOT NULL
		, TableName VARCHAR(200) NOT NULL
		, ColumnName VARCHAR(200) NOT NULL
		, ConstraintType VARCHAR(100) NOT NULL
	)
	INSERT INTO #tmpKeys(SchemaName, TableName, ColumnName, ConstraintType)
	SELECT CCU.TABLE_SCHEMA, CCU.TABLE_NAME, CCU.COLUMN_NAME, TC.CONSTRAINT_TYPE 
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
	JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME


	-- =======================================================================================
	-- Get a cursor for all of the tables to get MS_Description for each table and column

	DECLARE cur_tables INSENSITIVE CURSOR FOR
			SELECT T.object_id, S.name, T.name
			FROM sys.tables AS T
			JOIN sys.schemas AS S ON S.schema_id = T.schema_id
			WHERE T.name LIKE @TableNameLike
			ORDER BY T.name

	DECLARE @id INT, @current_schema_name VARCHAR(255), @current_table_name VARCHAR(256)
	OPEN cur_tables
	FETCH NEXT FROM cur_tables INTO @id, @current_schema_name, @current_table_name
	WHILE @@fetch_status <> -1
		BEGIN
			-- =======================================================================================
			-- Insert the descriptions for the COLUMNS in the current table into #tmpDescColumns

			INSERT INTO #tmpDescColumns (SchemaName, TableName, ColumnName, Description)
			SELECT    @current_schema_name, @current_table_name, objname, Convert(varchar(4000), value)
			FROM   fn_listextendedproperty ('MS_Description', 'schema', @current_schema_name, 'table', @current_table_name, 'column', NULL)

			-- =======================================================================================
			-- Insert the descriptions for the current TABLE into #tmpDescColumns
					
			INSERT INTO #tmpDescColumns (SchemaName, TableName, ColumnName, Description)
			SELECT    @current_schema_name, @current_table_name, '', Convert(varchar(4000), value)
			FROM   fn_listextendedproperty ('MS_Description', 'schema', @current_schema_name, 'table', @current_table_name, NULL, NULL)

			FETCH NEXT FROM cur_tables INTO @id, @current_schema_name, @current_table_name
		END

	CLOSE cur_tables
	DEALLOCATE cur_tables

	-- =======================================================================================
	-- Populate

	IF (@ReportType = 'combined' OR @ReportType = 'columns')
	BEGIN 
		DROP TABLE IF EXISTS #tmpOutput 
		CREATE TABLE #tmpOutput (
			  SchemaName varchar(100)
			, TableName VARCHAR(100)
			, ColumnName VARCHAR(100)
			, column_id INT 
			, DataTypeSpec VARCHAR(100)
			, Nullable VARCHAR(20)
			, [Identity] VARCHAR(20)
			, [Default] VARCHAR(100)
			, [Key] VARCHAR(100)
			, RefersTo VARCHAR(255)
			, Description VARCHAR(MAX)
		)

		INSERT INTO #tmpOutput (SchemaName, TableName, ColumnName, DataTypeSpec, Nullable, [Identity], [Default], [Key], RefersTo, column_id, Description)
		SELECT 
			  S.name AS SchemaName
			, T.name AS TableName
			, C.name AS ColumnName
			, CASE 
				WHEN Y.name IN ('varchar', 'nvarchar') AND C.max_length = -1 THEN UPPER(Y.name) + '(MAX)'
				WHEN Y.name IN ('char', 'varchar', 'datetime2') THEN UPPER(Y.name) + '(' + CONVERT(VARCHAR(10), C.max_length) + ')'
				WHEN Y.name IN ('nvarchar', 'nchar') THEN UPPER(Y.name) + '(' + CONVERT(VARCHAR(10), C.max_length/2) + ')'
				WHEN Y.name IN ('float', 'numeric', 'decimal') THEN UPPER(Y.name) + '(' + CONVERT(VARCHAR(10), C.precision) + ', ' + CONVERT(VARCHAR(10), C.scale) + ')'
				ELSE UPPER(Y.name) END AS DataTypeSpec
			, CASE C.is_nullable WHEN 0 THEN 'NOT NULL' ELSE 'NULL' END AS Nullable
			, CASE C.is_identity WHEN 1 THEN 'IDENTITY' ELSE '' END AS [Identity]
			, CASE WHEN DC.name IS NULL THEN ''
				--ELSE 'CONSTRAINT ' + DC.name + ' DEFAULT ' + SUBSTRING(DC.definition, 2, LEN(DC.definition)-2 ) END AS [Default] 
				ELSE SUBSTRING(DC.definition, 2, LEN(DC.definition)-2 ) END AS [Default] 
			, CASE WHEN K.ConstraintType = 'PRIMARY KEY' THEN 'PK' 
					WHEN K.ConstraintType = 'FOREIGN KEY' THEN 'FK' ELSE '' END AS [Key]
			, ISNULL(refT.name + '.' + refC.name, '') AS RefersTo
			, C.column_id
			, ISNULL(TDC.Description, '') AS Description
			--, '--' AS Divider, Y.name AS DataType, C.max_length, C.precision, C.scale, C.system_type_id, C.user_type_id
		FROM sys.tables AS T
		JOIN sys.schemas AS S ON S.schema_id = T.schema_id
		JOIN sys.columns AS C ON C.object_id = T.object_id
		JOIN sys.types AS Y ON Y.user_type_id = C.user_type_id
		LEFT OUTER JOIN sys.default_constraints AS DC ON DC.parent_object_id = T.object_id AND DC.parent_column_id = C.column_id
		LEFT OUTER JOIN sys.foreign_key_columns AS FKC ON FKC.parent_object_id = C.object_id AND FKC.parent_column_id = C.column_id
		LEFT OUTER JOIN sys.tables AS refT ON refT.object_id = FKC.referenced_object_id
		LEFT OUTER JOIN sys.columns AS refC ON refC.object_id = FKC.referenced_object_id AND FKC.referenced_column_id = refC.column_id
		LEFT OUTER JOIN #tmpKeys AS K ON K.SchemaName = S.name AND K.TableName = T.name AND K.ColumnName = C.name
		LEFT OUTER JOIN #tmpDescColumns AS TDC ON TDC.SchemaName = S.name AND TDC.TableName = T.name AND TDC.ColumnName = c.name
		WHERE T.name <> 'sysdiagrams'
		AND T.name LIKE @TableNameLike
		AND C.name LIKE @ColumnNameLike
	END 
	
	-- =======================================================================================
	IF (@ReportType = 'combined')
	BEGIN 
	-- Row for each table
		INSERT INTO #tmpOutput (SchemaName, TableName, ColumnName, DataTypeSpec, Nullable, [Identity], [Default], [Key], RefersTo, column_id, Description)
		SELECT S.name AS SchemaName
			, T.name AS Tablename
				, '', '', '', '', '', '', '', '' AS column_id, ISNULL(TDC.Description, '')
		FROM sys.tables AS T
		JOIN sys.schemas AS S ON S.schema_id = T.schema_id
		LEFT OUTER JOIN #tmpDescColumns AS TDC ON TDC.SchemaName = S.name AND TDC.TableName = T.name AND TDC.ColumnName = ''
		WHERE T.name LIKE @TableNameLike
	END 

	-- =======================================================================================
	-- Final SELECT for 'combined' or 'columns'
	IF (@ReportType = 'combined' OR @ReportType = 'columns')
	BEGIN
		SELECT * FROM #tmpOutput
		ORDER BY SchemaName, TableName, column_id
	END 

	-- =======================================================================================
	-- Populate and final SELECT for 'tables'
	IF (@ReportType = 'tables')
	BEGIN 
		
		DROP TABLE IF EXISTS #tmpTableNames
		DROP TABLE IF EXISTS #tmpSpaceUsed
		DROP TABLE IF EXISTS #tmpTableStats

		-- #tmpTableNames holds table names of all tables in the current DB
		CREATE TABLE #tmpTableNames ( SchemaName VARCHAR(100), TableName VARCHAR(100), object_id INT, NumCols INT)

		-- #tmpSpaceUsed holds the results of the sp_spaceused query for each table
		--CREATE TABLE #tmpSpaceUsed ( name VARCHAR(100), Rows INT, Reserved VARCHAR(50), Data VARCHAR(50), index_size VARCHAR(50), Unused VARCHAR(50) )
		CREATE TABLE #tmpSpaceUsed ( tmpID INT NOT NULL IDENTITY, SchemaName VARCHAR(100) NULL, TableName VARCHAR(100) NULL, Rows INT NULL, Reserved VARCHAR(50) NULL, Data VARCHAR(50) NULL, index_size VARCHAR(50) NULL, Unused VARCHAR(50) NULL )

		CREATE TABLE #tmpTableStats (
			TableStatID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY
			, ObjectID INT NULL
			, SchemaName VARCHAR(128) NULL
			, TableName VARCHAR(128) NULL
			, [Rows] INT NULL
			, NumCols INT NULL
			, ReservedKB INT NULL
			, DataKB INT NULL
			, IndexSizeKB INT NULL
			, UnusedKB INT NULL
		);

		-- =============================================================================
		-- cursor through each table in the database
		INSERT INTO #tmpTableNames 
		SELECT S.name, T.name, T.object_id, COUNT(*) 
		FROM sys.tables AS T 
		JOIN sys.schemas AS S ON S.schema_id = T.schema_id 
		JOIN sys.columns AS C ON C.object_id = T.object_id 
		WHERE T.name LIKE @TableNameLike
		GROUP BY S.name, T.name, T.object_id
		
		DECLARE curTables CURSOR LOCAL FORWARD_ONLY FOR
			SELECT SchemaName, TableName, object_id 
			FROM #tmpTableNames 
			WHERE TableName LIKE @TableNameLike
			ORDER BY TableName
		DECLARE @object_id	INT
		DECLARE @SchemaName VARCHAR(128)
		DECLARE @TableName	VARCHAR(128)
		DECLARE @sql		NVARCHAR(2000)

		OPEN curTables
		FETCH NEXT FROM curTables INTO @SchemaName, @TableName, @object_id
		WHILE @@fetch_status <> -1
			BEGIN
				--SELECT @sql = N'USE [' + @DatabaseName + '] INSERT INTO #tmpSpaceUsed EXEC sp_spaceused ''[' + @SchemaName + '].[' + @TableName + ']'''
				SELECT @sql = N'INSERT INTO #tmpSpaceUsed (TableName, Rows, Reserved, Data, index_size, Unused) EXEC sp_spaceused [' + @SchemaName + '.' + REPLACE(@TableName, '''', '''''') + ']'
				PRINT @sql
				EXEC (@sql)
				FETCH NEXT FROM curTables INTO @SchemaName, @TableName, @object_id
			END
		CLOSE curTables
		DEALLOCATE curTables

		-- =============================================================================
		-- Trim off the <space>KB so we get integers
		UPDATE #tmpSpaceUsed SET 
				Reserved = REPLACE(Reserved, ' KB', '')			
			, Data = REPLACE(Data, ' KB', '')
			, index_size = REPLACE(index_size, ' KB', '')
			, Unused = REPLACE(Unused, ' KB', '')			

		-- Finally populate our TableStats table with our results.
		INSERT INTO #tmpTableStats (ObjectID, SchemaName, TableName, Rows, NumCols, ReservedKB, DataKB,IndexSizeKB, UnusedKB)
		SELECT T.object_id, T.SchemaName, T.TableName, X.Rows, T.NumCols, X.Reserved, X.Data, X.index_size, X.Unused
		FROM #tmpSpaceUsed AS X
		--JOIN #tmpTableNames AS T ON X.name = '[' + T.SchemaName + '].[' + T.TableName + ']'
		JOIN #tmpTableNames AS T ON X.TableName = T.TableName;  -- Pre-SQL2016 JOIN

		SELECT TS.*, TDC.Description 
		FROM #tmpTableStats AS TS
		LEFT OUTER JOIN #tmpDescColumns AS TDC ON TDC.SchemaName = TS.SchemaName AND TDC.TableName = TS.TableName AND TDC.ColumnName = ''
		ORDER BY TS.SchemaName, TS.TableName
	END -- IF (@ReportType = 'tables')

	-- =======================================================================================
	-- cleanup
	DROP TABLE IF EXISTS #tmpDescColumns 
	DROP TABLE IF EXISTS #tmpOutput 
	DROP TABLE IF EXISTS #tmpTableStats
	DROP TABLE IF EXISTS #tmpKeys 
	DROP TABLE IF EXISTS #tmpSpaceUsed 
END 
GO
