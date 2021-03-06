CREATE OR ALTER PROCEDURE dbo.adm_Document_Tables2

-- =======================================================================================
-- Desc:	Selects meta data about tables/columns including the MS_Description extended propertes (if any) in each table/column
-- License:	ShoutOutWare - give me a shout out on Twitter @bdill if this script helped you. :) (retain comments and do not redistribute)
-- Auth: 	Brian Dill 2021-03-16
-- Script home: https://github.com/wbdill/SQLServer-sandbox01/blob/master/adm_Document_Tables2.sql
-- Other useful scripts: https://github.com/wbdill/SQLServer-sandbox01

-- Note:	Paste this in cell L2 of a dump to Excel to get SQL code to update the MS_Description extended properties in SQL
--          ="EXEC dbo.adm_TableAndColumnExtendedProperty_ups '"&A2&"', '"&B2&"', '"&C2&"', '"&K2&"'"

  @TableNameLike VARCHAR(100) = '%'
, @ColumnNameLike VARCHAR(100) = '%'

AS
BEGIN 
	SET NOCOUNT ON

	-- =======================================================================================
	-- Create a temp table to hold all of the descriptions for alter joining.

	IF OBJECT_ID('tempdb..#tmpDescColumns') IS NOT NULL 
		DROP TABLE #tmpDescColumns 

	CREATE TABLE #tmpDescColumns (  
		  DescID               INT IDENTITY(1,1) NOT NULL
		, SchemaName           VARCHAR(255) NULL
		, TableName            VARCHAR(100)  NULL
		, ColumnName           VARCHAR(100)  NULL
		, Description          VARCHAR(2000) NULL
	)

	-- =======================================================================================
	-- temp table for all PKs FKs
	
	IF OBJECT_ID('tempdb..#tmpKeys') IS NOT NULL 
		DROP TABLE #tmpKeys 

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
			-- Insert the descriptions for the columns in the current table into #tmpDescColumns

			INSERT INTO #tmpDescColumns (SchemaName, TableName, ColumnName, Description)
			SELECT    @current_schema_name, @current_table_name, objname, Convert(varchar(2000), value)
			FROM   fn_listextendedproperty ('MS_Description', 'schema', @current_schema_name, 'table', @current_table_name, 'column', NULL)

			-- =======================================================================================
			-- Insert the descriptions for the current table into #tmpDescColumns
					
			INSERT INTO #tmpDescColumns (SchemaName, TableName, ColumnName, Description)
			SELECT    @current_schema_name, @current_table_name, '', Convert(varchar(2000), value)
			FROM   fn_listextendedproperty ('MS_Description', 'schema', @current_schema_name, 'table', @current_table_name, NULL, NULL)

			FETCH NEXT FROM cur_tables INTO @id, @current_schema_name, @current_table_name
		END

	CLOSE cur_tables
	DEALLOCATE cur_tables

	-- =======================================================================================
	-- Final SELECT

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
			
	UNION

	-- Row for each table
	SELECT S.name AS SchemaName
		, T.name AS Tablename
		, '', '', '', '', '', '', '', '' AS column_id, ISNULL(TDC.Description, '') AS Description
	FROM sys.tables AS T
	JOIN sys.schemas AS S ON S.schema_id = T.schema_id
	LEFT OUTER JOIN #tmpDescColumns AS TDC ON TDC.SchemaName = S.name AND TDC.TableName = T.name AND TDC.ColumnName = ''
	WHERE T.name LIKE @TableNameLike
	ORDER BY S.name, T.name, C.column_id

	-- =======================================================================================
	-- cleanup
	DROP TABLE #tmpDescColumns

END 
