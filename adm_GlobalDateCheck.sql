-- Desc: Checks all date-like columns and reports a count of violations per column as well as listing individual violator dates.
-- Note: The count report (first result table) will always work, but the individual violator capture (2nd report table) assumes the table has a single-column PK
-- Note: Search for --<< Customize to see where you can filter out columns and/or tables.
-- 2024-06-26 - bdill
CREATE OR ALTER PROCEDURE dbo.adm_GlobalDateRangeCheck
	  @LowerDate DATE = '1900-01-01'
	, @UpperDate DATE = '2100-01-01'
	, @MaxViolatorsPerTable INT  = 1000 -- how many "bad" dates do you want to log for each table?
AS
BEGIN
	-- ================================================================================
	IF OBJECT_ID('tempdb..#tmpDateColumnChecks') IS NOT NULL 
		DROP TABLE #tmpDateColumnChecks
	CREATE TABLE #tmpDateColumnChecks (
		  ID INT NOT NULL IDENTITY
		, TableName VARCHAR(100) NOT NULL
		, IDColumnName VARCHAR(100) NULL
		, DateColumnName VARCHAR(100) NOT NULL
		, DataType VARCHAR(100) NOT NULL
		, ViolationRowCount INT NULL
		, SqlCheck NVARCHAR(MAX) NULL
		, SqlInsert NVARCHAR(MAX) NULL
	)
	IF OBJECT_ID('tempdb..#tmpDateColumnViolations') IS NOT NULL 
		DROP TABLE #tmpDateColumnViolations
	CREATE TABLE #tmpDateColumnViolations (
		  ID INT NOT NULL IDENTITY
		, TableName VARCHAR(100) NOT NULL
		, IDColumnName VARCHAR(100) NULL
		, DateColumnName VARCHAR(100) NULL
		, IDValue VARCHAR(100) NULL
		, DateValue DATETIME2(2) NULL
	)

	DECLARE @TableName VARCHAR(100)
	DECLARE @IDColumnName VARCHAR(100)
	DECLARE @DateColumnName VARCHAR(100)
	DECLARE @DataType VARCHAR(100)
	DECLARE @SqlCount NVARCHAR(MAX)
	DECLARE @SqlCheck NVARCHAR(MAX)
	DECLARE @SqlInsert NVARCHAR(MAX)
	DECLARE @RowCount INT 

	-- ================================================================================
	-- Cursor to get all the date-like columns and a count of dates that are out of range
	DECLARE cur CURSOR FAST_FORWARD FOR
		SELECT CONCAT(S.name, '.', T.name) AS TableName
		, PK.COLUMN_NAME AS IDColumnName
		, C.name AS DateColumnName
		, Y.name
		, CONVERT(NVARCHAR(MAX), CONCAT('SELECT @RowsOut = COUNT(*) FROM ', S.name, '.[', T.name, '] WHERE [', C.name
										, '] < ''', @LowerDate, ''' OR [', C.name, '] > ''', @UpperDate, '''')) AS SqlCount
		, CONVERT(NVARCHAR(MAX), CONCAT(N'SELECT TOP (1000) ', ISNULL(PK.COLUMN_NAME, 'NULL'), ', CONVERT(DATE, [', C.name, ']) AS '
										, C.name , ' FROM ', S.name, '.[', T.name, '] WHERE [', C.name, '] < ''', @LowerDate, ''' OR ['
										, C.name, '] > ''', @UpperDate, ''' ORDER BY [', C.name, ']')) AS SqlCheck
		, CONVERT(NVARCHAR(MAX), CONCAT(N'INSERT INTO #tmpDateColumnViolations SELECT TOP (', @MaxViolatorsPerTable, ') '''
										, S.name, '.', T.name, ''', ''', PK.COLUMN_NAME, ''', ''', C.name, ''', '
										, ISNULL(PK.COLUMN_NAME, 'NULL'), ', CONVERT(DATE, [', C.name, ']) AS ', C.name , ' FROM '
										, S.name, '.[', T.name, '] WHERE [', C.name, '] < ''', @LowerDate, ''' OR ['
										, C.name, '] > ''', @UpperDate, ''' ORDER BY [', C.name, ']')) AS SqlInsert
		FROM sys.schemas AS S
		JOIN sys.tables AS T ON T.schema_id = S.schema_id
		JOIN sys.columns AS C ON C.object_id = T.object_id
		JOIN sys.types AS Y ON Y.user_type_id = C.user_type_id
		LEFT OUTER JOIN (
			SELECT CCU.TABLE_SCHEMA, CCU.TABLE_NAME, CCU.COLUMN_NAME, TC.CONSTRAINT_TYPE
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
			JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
			WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
		) AS PK ON PK.TABLE_SCHEMA = S.name AND PK.TABLE_NAME = T.name
		WHERE Y.name IN ('date', 'datetime', 'datetime2', 'smalldate')
		AND S.Name NOT IN ('hst')                                          --<< Customize to filter out tables/columns you don't care to check
		AND C.name NOT IN ('Created', 'Modified', 'CreateDate', 'ModDate') --<< Customize to filter out tables/columns you don't care to check
		AND (T.name NOT LIKE 'tmp%' AND T.name NOT LIKE 'ztmp%')           --<< Customize to filter out tables/columns you don't care to check
		ORDER BY TableName, DateColumnName

	OPEN cur
	FETCH NEXT FROM cur INTO @TableName, @IDColumnName, @DateColumnName, @DataType, @SqlCount, @SqlCheck, @SqlInsert
	WHILE @@FETCH_STATUS = 0
	BEGIN 
		PRINT @SqlCheck
		DECLARE @ParmDef NVARCHAR(500) = N'@RowsOut INT OUTPUT'
		DECLARE @Rows INT
		EXEC sp_executesql @SqlCount, @ParmDef, @RowsOut = @Rows OUTPUT
		SET @RowCount = @rows-- @@ROWCOUNT
		
		INSERT INTO #tmpDateColumnChecks (TableName, IDColumnName, DateColumnName, DataType, ViolationRowCount, SqlCheck, SqlInsert)
		VALUES (@TableName, @IDColumnName, @DateColumnName, @DataType, @RowCount, @SqlCheck, @SqlInsert)

		FETCH NEXT FROM cur INTO @TableName, @IDColumnName, @DateColumnName, @DataType, @SqlCount, @SqlCheck, @SqlInsert
	END
	CLOSE cur
	DEALLOCATE cur

	-- ================================================================================
	-- Populate #tmpDateColumnChecks with dates that are out of range
	DECLARE @SqlInsert2 NVARCHAR(MAX)
	DECLARE cur2 CURSOR FAST_FORWARD FOR
		SELECT SqlInsert FROM #tmpDateColumnChecks 
		WHERE ViolationRowCount > 0
		ORDER BY ViolationRowCount DESC
	OPEN cur2
	FETCH NEXT FROM cur2 INTO @SqlInsert2
	WHILE @@FETCH_STATUS = 0
	BEGIN 
		PRINT @SqlInsert2
		EXEC sp_executesql @stmt = @SqlInsert2
		FETCH NEXT FROM cur2 INTO @SqlInsert2
	END
	CLOSE cur2
	DEALLOCATE cur2

	-- ================================================================================
	SELECT * FROM #tmpDateColumnChecks 
	ORDER BY ViolationRowCount DESC, TableName

	SELECT * FROM #tmpDateColumnViolations 
	ORDER BY TableName, DateColumnName, IDValue
END
-- ================================================================================
GO


RETURN 
-- ================================================================================