-- Desc: Exploratory data analysis proc to get count of non-empty columns for specified schema and table
-- 2024-10-03 bdill - Initial write
CREATE OR ALTER PROC dbo.adm_EDA_Count_NonEmpty_and_Distinct_ForAllColumns
  @SchemaNameLike VARCHAR(100) = '%'
, @TableNameLike VARCHAR(100) = '%'
, @ColumnNameLike VARCHAR(100) = '%'
AS
BEGIN
	SET NOCOUNT ON
	-- Dummy proofing
	IF @SchemaNameLike = ''
		SET @SchemaNameLike = '%'
	IF @TableNameLike = ''
		SET @TableNameLike = '%'
	IF @ColumnNameLike = ''
		SET @ColumnNameLike = '%'

	-- Local vars
	DECLARE @SchemaName VARCHAR(100) 
	DECLARE @TableName VARCHAR(100)
	DECLARE @ColumnName VARCHAR(100)
	DECLARE @ColID INT
	DECLARE @Nullable VARCHAR(10)

	DECLARE @SqlTableCount NVARCHAR(MAX)
	DECLARE @TableCount INT 
	DECLARE @SqlNonEmptyCount NVARCHAR(MAX)
	DECLARE @NonEmptyCount INT
	DECLARE @SqlDistinctCount NVARCHAR(MAX)
	DECLARE @DistinctCount INT 

	-- Tmp table for results
	IF OBJECT_ID('tempdb..#tmpColAnalysisResults') IS NOT NULL	
		DROP TABLE #tmpColAnalysisResults

	CREATE TABLE #tmpColAnalysisResults ( 
		  SchemaName VARCHAR(100) NOT NULL
		, TableName VARCHAR(100) NOT NULL
		, ColumnName VARCHAR(100) NOT NULL
		, ColID INT NULL
		, Nullable VARCHAR(10) NULL
		, TableCount INT NULL
		, NonEmptyCount INT NULL
		, DistinctCount INT NULL
		--, SqlCount NVARCHAR(MAX) NULL
	)

	DECLARE cur CURSOR FAST_FORWARD FOR 
		SELECT S.name AS SchemaName
			, T.name AS TableName
			, C.name AS ColumnName
			, C.column_id AS ColID
			, CASE WHEN C.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END AS Nullable
			, CONCAT('SELECT @RowsOut = COUNT(*) FROM ', S.name, '.', T.name) AS SqlTableCount
			, CASE WHEN Y.name IN ('char', 'nchar', 'varchar', 'nvarchar', 'text') 
				THEN CONCAT('SELECT @RowsOut = COUNT(*) FROM ', S.name, '.', T.name, ' WHERE [', C.name, '] <> ''''')      -- string-like data types WHERE col <> ''
				ELSE CONCAT('SELECT @RowsOut = COUNT(*) FROM ', S.name, '.', T.name, ' WHERE [', C.name, '] IS NOT NULL') -- all others             WHERE col IS NOT NULL
				END AS SqlNonEmptyCount
			, CONCAT('SELECT @RowsOut = COUNT(DISTINCT [', C.name, ']) FROM ', S.name, '.', T.name) AS SqlDistinctCount
		FROM sys.schemas AS S
		JOIN sys.tables AS T ON T.schema_id = S.schema_id
		JOIN sys.columns AS C ON C.object_id = T.object_id
		JOIN sys.types AS Y ON Y.user_type_id = C.user_type_id
		WHERE 1 = 1
		AND S.name LIKE @SchemaNameLike
		AND T.name LIKE @TableNameLike
		AND C.name LIKE @ColumnNameLike

	OPEN cur
	FETCH NEXT FROM cur INTO @SchemaName, @TableName, @ColumnName, @ColID, @Nullable, @SqlTableCount, @SqlNonEmptyCount, @SqlDistinctCount
	WHILE @@FETCH_STATUS = 0
	BEGIN
		--SELECT @SchemaName, @TableName, @ColumnName, @SqlCount, @NonEmptyCount
		DECLARE @ParmDef NVARCHAR(500) = N'@RowsOut INT OUTPUT'
		DECLARE @Rows INT

		EXEC sp_executesql @SqlTableCount, @ParmDef, @RowsOut = @Rows OUTPUT
		SET @TableCount = @rows

		EXEC sp_executesql @SqlNonEmptyCount, @ParmDef, @RowsOut = @Rows OUTPUT
		SET @NonEmptyCount = @rows

		EXEC sp_executesql @SqlDistinctCount, @ParmDef, @RowsOut = @Rows OUTPUT
		SET @DistinctCount = @rows
		
		INSERT INTO #tmpColAnalysisResults ( SchemaName,  TableName,  ColumnName,  ColID,  Nullable,  TableCount,  NonEmptyCount,  DistinctCount)
		VALUES                             (@SchemaName, @TableName, @ColumnName, @ColID, @Nullable, @TableCount, @NonEmptyCount, @DistinctCount)
		
		FETCH NEXT FROM cur INTO @SchemaName, @TableName, @ColumnName, @ColID, @Nullable, @SqlTableCount, @SqlNonEmptyCount, @SqlDistinctCount
	END
	CLOSE cur
	DEALLOCATE cur
	
	SELECT SchemaName
		, TableName
		, ColumnName
		, ColID
		, Nullable
		, CASE WHEN NonEmptyCount = 0 THEN 'ALL EMPTY' 
			WHEN NonEmptyCount = TableCount THEN 'Full'
			ELSE '' END AS EmptyIndicator
		, NonEmptyCount
		, DistinctCount
		, TableCount
		, CONVERT(DECIMAL(5,2), NonEmptyCount * 1.0 / TableCount * 100) AS PctNonEmpty
		, CONVERT(DECIMAL(5,2), DistinctCount * 1.0 / TableCount * 100) AS PctDistinct
--		, CASE WHEN DistinctCount < 50 AND NonEmptyCount > 0 THEN 
--			CONCAT('SELECT ', ColumnName, ', COUNT(*) AS N FROM ', SchemaName, '.', TableName, ' GROUP BY ', ColumnName, ' ORDER BY ', ColumnName)
--			ELSE '' END AS SqlGroupBy
	FROM #tmpColAnalysisResults
	ORDER BY SchemaName, TableName, ColID

	-- --------------------------------------------------------------------------------
	-- Group By
	SELECT SchemaName
		, TableName
		, ColumnName
		, ColID
		, DistinctCount
		, TableCount
		, CONCAT('SELECT ', ColumnName, ', COUNT(*) AS N FROM ', SchemaName, '.', TableName, ' GROUP BY ', ColumnName, ' ORDER BY ', ColumnName) SqlGroupBy
	FROM #tmpColAnalysisResults
	WHERE DistinctCount < 50
	AND NonEmptyCount > 0
	ORDER BY SchemaName, TableName, ColID

	-- --------------------------------------------------------------------------------
	-- Only Empty cols
	SELECT SchemaName
		, TableName
		, ColumnName
		, ColID
		, 'ALL EMPTY' AS EmptyIndicator
		, DistinctCount
		, TableCount
	FROM #tmpColAnalysisResults
	WHERE NonEmptyCount = 0
	ORDER BY SchemaName, TableName, ColID
END
GO