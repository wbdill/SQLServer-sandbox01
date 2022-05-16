-- =======================================================================================
-- Desc: Queries all date-based columns for existence of future dates
-- License:	ShoutOutWare - give me a shout out on Twitter @bdill if this script helped you. :) (retain comments and do not redistribute)
-- Auth: 	Brian Dill 2021-04-28
-- Script home: https://github.com/wbdill/SQLServer-sandbox01/blob/master/adm_DateColsFutureDateCheck.sql
-- Other useful scripts: https://github.com/wbdill/SQLServer-sandbox01

-- =======================================================================================
-- Create a perma-temp table to store results
DROP TABLE IF EXISTS dbo.ztmp_DateColsFutureDateCheck
CREATE TABLE dbo.ztmp_DateColsFutureDateCheck ( 
	  ID INT NOT NULL IDENTITY CONSTRAINT PK_ztmp_DateColsFutureDateCheck PRIMARY KEY
	, SchemaName VARCHAR(200) NULL
	, TableName VARCHAR(200) NULL
	, ColName VARCHAR(200) NULL
	, DataType VARCHAR(50) NULL
	, NumOfFutureDates INT NULL 
	, SqlSelect VARCHAR(2000) NULL
	, Created DATETIME2(3) NULL CONSTRAINT DF_ztmp_DateColsFutureDateCheck_Created DEFAULT GETDATE()
)

-- =======================================================================================
-- Populate ztmp table with a cursor.
SET NOCOUNT ON
DECLARE @SqlIns NVARCHAR(2000)
DECLARE @NumOfFutureDates INT 
DECLARE @SchemaName VARCHAR(200)
DECLARE @TableName VARCHAR(200)
DECLARE @ColName VARCHAR(200)
DECLARE @DataType VARCHAR(50)
DECLARE @SqlSelectCount NVARCHAR(2000)
DECLARE @SqlSelectRows NVARCHAR(2000)
DECLARE cur001 CURSOR LOCAL FAST_FORWARD READ_ONLY FOR 
	SELECT S.name AS SchemaName
		, T.Name AS TableName
		, C.name AS ColumnName
		, Y.name AS DataType
		, 'SELECT @retValOUT = COUNT(*) FROM ' + S.name + '.' + T.name + ' WHERE ' + C.name + ' > GETDATE()' AS SqlSelectCount
		, 'SELECT * FROM ' + S.name + '.' + T.name + ' WHERE ' + C.name + ' > GETDATE()' AS SqlSelectRows
	FROM sys.schemas AS S
	JOIN sys.tables AS T ON T.schema_id = S.schema_id
	JOIN sys.columns AS C ON C.object_id = T.object_id
	JOIN sys.types AS Y ON Y.user_type_id = C.user_type_id
	WHERE Y.name IN ('datetime', 'datetime2', 'date', 'smalldatetime')
	--AND T.name IN ('Users', 'MyOtherTableName')
	ORDER BY S.name, T.name, C.name
OPEN cur001
FETCH NEXT FROM cur001 INTO @SchemaName, @TableName, @ColName, @DataType, @SqlSelectCount, @SqlSelectRows
WHILE @@FETCH_STATUS = 0
BEGIN
	-- Get the COUNT(*) of rows with a future date for specified table/column
	DECLARE @ParmDefinition nvarchar(500) = N'@retValOUT INT OUTPUT'
	EXEC dbo.sp_executesql @SqlSelectCount, @ParmDefinition, @retvalOut = @NumOfFutureDates OUTPUT

	PRINT 'NumOfFutureDates:' + CONVERT(VARCHAR(50), @NumOfFutureDates) + ' for ' + @SchemaName + '.' + @TableName + ' ' + @ColName

	SET @SqlIns = N'INSERT INTO dbo.ztmp_DateColsFutureDateCheck (SchemaName, TableName, ColName, DataType, NumOfFutureDates, SqlSelect) '
			+ ' VALUES (''' +@SchemaName + ''', ''' + @TableName + ''', '''+ @ColName + ''', '''+ @DataType + ''', ' + CONVERT(VARCHAR(50), @NumOfFutureDates) + ', ''' + @SqlSelectRows + ''');'

	EXEC dbo.sp_executesql @SqlIns  -- Populate the ztmp table

	FETCH NEXT FROM cur001 INTO @SchemaName, @TableName, @ColName, @DataType, @SqlSelectCount, @SqlSelectRows
END
CLOSE cur001
DEALLOCATE cur001

-- Look at all the columns with at least 1 future date
SELECT * FROM dbo.ztmp_DateColsFutureDateCheck WHERE NumOfFutureDates > 0



GO
