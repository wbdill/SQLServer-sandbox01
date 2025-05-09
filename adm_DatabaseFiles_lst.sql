-- Desc: Gets all database files on a server, size, spaceused, percent used/free, growth
-- Auth: Brian Dill 2025-04-29 (assist from Claude 3.7)
-- License: ShoutOutWare - give me a shout out on Twitter @bdill or Bluesky @wbdill if this script helped you. :)  (retain comments and do not redistribute)
-- Script home: https://github.com/wbdill/SQLServer-sandbox01/adm_DatabaseFiles_lst.sql
-- Other useful files: https://github.com/wbdill/SQLServer-sandbox01

DECLARE @SQL NVARCHAR(MAX);
SET @SQL = N'
USE ?  -- loop via sp_MSforeachdb
INSERT INTO #tmpSysDBFiles (DateChecked, ServerName, DatabaseName, LogicalName, PhysicalName, FileType, TotalSizeMB, UsedSpaceMB, AvailableSpaceMB, PercentUsed, PercentFree, MaxSizeMB, GrowthIncrement)
SELECT GETDATE() AS DateChecked 
    , @@ServerName as ServerName
    , DB_NAME() AS [DatabaseName]
    , [name] AS [LogicalName]
    , physical_name AS [PhysicalName]
    , CASE type_desc WHEN ''ROWS'' THEN ''Data File'' WHEN ''LOG'' THEN ''Log File'' ELSE type_desc END AS [FileType]
    , CONVERT(DECIMAL(15,2), size * 8 / 1024.0) AS [TotalSizeMB]
    , CONVERT(DECIMAL(15,2), FILEPROPERTY(name, ''SpaceUsed'') * 8 / 1024.0) AS [UsedSpaceMB]
    , CONVERT(DECIMAL(15,2), (size - FILEPROPERTY(name, ''SpaceUsed'')) * 8 / 1024.0) AS [AvailableSpaceMB]
    , CONVERT(DECIMAL(5,2), (CAST(FILEPROPERTY(name, ''SpaceUsed'') AS FLOAT) * 100 / CAST(size AS FLOAT)) ) AS [PercentUsed]
    , CONVERT(DECIMAL(5,2), (( CAST(size AS FLOAT) - CAST(FILEPROPERTY(name, ''SpaceUsed'') AS FLOAT) ) * 100 / CAST(size AS FLOAT)) ) AS [PercentFree]
    , CASE max_size
        WHEN 0 THEN ''No Growth''
        WHEN -1 THEN ''Unlimited''
        ELSE CONVERT(VARCHAR, CONVERT(DECIMAL(15,2), CONVERT(BIGINT, max_size) * 8 / 1024.0)) --+ '' MB''
      END AS [MaxSizeMB]
    , CASE is_percent_growth
        WHEN 1 THEN CONVERT(VARCHAR, growth) + ''%''
        ELSE CONVERT(VARCHAR, CONVERT(DECIMAL(15,2), growth * 8 / 1024.0)) + '' MB''
      END AS [GrowthIncrement]
FROM sys.database_files;'

IF OBJECT_ID('tempdb..#tmpSysDBFiles', 'U') IS NOT NULL
	DROP TABLE #tmpSysDBFiles;

CREATE TABLE #tmpSysDBFiles (
	  DateChecked DATETIME2(0) NOT NULL CONSTRAINT DF_tmpSysDBFiles_DateChecked DEFAULT GETDATE()
	, ServerName VARCHAR(100) NOT NULL
	, DatabaseName VARCHAR(100) NULL
	, LogicalName VARCHAR(100) NOT NULL
	, FileType VARCHAR(50) NOT NULL
	, TotalSizeMB DECIMAL(12,2) NULL
	, UsedSpaceMB DECIMAL(12,2) NULL
    , AvailableSpaceMB DECIMAL(12,2) NULL
    , PercentUsed DECIMAL(12,2) NULL
    , PercentFree DECIMAL(12,2) NULL
    , MaxSizeMB VARCHAR(100) NULL
    , GrowthIncrement VARCHAR(100) NULL
    , PhysicalName VARCHAR(500) NOT NULL
)
EXEC sp_MSforeachdb @SQL

SELECT * FROM #tmpSysDBFiles;

DROP TABLE #tmpSysDBFiles;