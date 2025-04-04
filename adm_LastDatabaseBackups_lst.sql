-- =============================================================================
-- Database backup info
-- Auth: bdill (W. Brian Dill) @bdill
-- Date: 2013-11-20
-- Upd:  2015-01-22 (bdill) getting last full AND log backup for display
-- Upd:  2025-04-03 (bdill) CAST(size AS BIGINT) to prevent arithmetic overflow
-- =============================================================================
ALTER PROCEDURE [dbo].[adm_LastDatabaseBackups_lst]

AS
BEGIN
	SET NOCOUNT ON;

	-- =============================================================================
	-- RANDA_Owner and RANDA_Description extended properties
	-- =============================================================================
	IF OBJECT_ID('tempdb..#tmpDBs') IS NULL
	BEGIN
		CREATE TABLE #tmpDBs ( 
			  DBName varchar(1000) NOT NULL
			, DataFileSizeMB BIGINT NULL
            , LogFileSizeMB BIGINT NULL 
			, LastFullBackup DATETIME NULL
			, LastFullBackupSizeMB BIGINT NULL
			, LastFullBackupUser VARCHAR(100) NULL
			, LastLogBackup DATETIME NULL
			, LastLogBackupSizeMB BIGINT NULL
			, LastLogBackupUser VARCHAR(100) NULL
			, Business_Owner VARCHAR(100) NULL
			, Business_Description VARCHAR(1000) NULL 
			, LastRestoreDate SMALLDATETIME NULL
			)
		INSERT INTO #tmpDBs ( DBName ) SELECT name FROM sys.databases ORDER BY name
	END
	DECLARE @dbname VARCHAR(1000) 
	DECLARE @sql NVARCHAR(max)
	DECLARE @tmpOwner VARCHAR(100)
	DECLARE @tmpDesc VARCHAR(1000)
	DECLARE @tmpDataFileSizeMB BIGINT 
	DECLARE @tmpLogFileSizeMB BIGINT 

	DECLARE curDBs CURSOR LOCAL FORWARD_ONLY FOR 
		SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' ORDER BY name
	OPEN curDBs
	FETCH NEXT FROM curDBs INTO @dbname
	WHILE @@FETCH_STATUS = 0
	BEGIN
	    -- =============================================================================
	    -- Business_Owner and Business_Description are extended properties to self-document the DBs
	    -- =============================================================================
		SET @sql = N'SELECT @tmpOwner = CONVERT(VARCHAR(100), value) FROM [' + @dbname + '].sys.extended_properties WHERE class_desc = ''DATABASE'' AND name = ''Business_Owner'' '
		EXEC sp_executesql @statement = @sql, @params = N'@tmpOwner varchar(100) OUTPUT', @tmpOwner = @tmpOwner OUTPUT

		SET @sql = N'SET @tmpDesc  = (SELECT CONVERT(VARCHAR(1000), value) FROM [' + @dbname + '].sys.extended_properties WHERE class_desc = ''DATABASE'' AND name = ''Business_Description'' )'
		EXEC sp_executesql @statement = @sql, @params = N'@tmpDesc varchar(1000) OUTPUT', @tmpDesc = @tmpDesc OUTPUT

		SET @tmpDesc  = ( SELECT ISNULL(REPLACE(@tmpDesc, '''', ''''''), '') )
		SET @tmpOwner = ( SELECT ISNULL(REPLACE(@tmpOwner, '''', ''''''), '') )


		-- =============================================================================
		-- Data and log size
		SET @sql = N'SET @tmpDataFileSizeMB = (SELECT SUM(CAST(size AS BIGINT) * 8) / 1024 FROM [' + @dbname + '].sys.database_files WHERE type_desc = ''ROWS'')' -- size is # of 8k pages
		EXEC sys.sp_executesql @statement = @sql, @params = N'@tmpDataFileSizeMB BIGINT OUTPUT', @tmpDataFileSizeMB = @tmpDataFileSizeMB OUTPUT 

		SET @sql = N'SET @tmpLogFileSizeMB = (SELECT SUM(CAST(size AS BIGINT) * 8) / 1024 FROM [' + @dbname + '].sys.database_files WHERE type_desc = ''LOG'')'
		EXEC sys.sp_executesql @statement = @sql, @params = N'@tmpLogFileSizeMB BIGINT OUTPUT', @tmpLogFileSizeMB = @tmpLogFileSizeMB OUTPUT 


		SET @sql = N'UPDATE #tmpDBs SET Business_Owner = ''' + @tmpOwner + ''''
					+ ' , Business_Description = ''' + @tmpDesc + ''''
					+ ' , DataFileSizeMB = ' + CONVERT(VARCHAR(100), @tmpDataFileSizeMB)
					+ ' , LogFileSizeMB = '  + CONVERT(VARCHAR(100), @tmpLogFileSizeMB) 
					+ ' WHERE DBName = ''' + @dbname + ''''

		PRINT @sql
		EXEC sys.sp_executesql @statement = @sql
		SET @tmpDesc = ''
		SET @tmpOwner = ''
	
		FETCH NEXT FROM curDBs INTO @dbname
	END
	CLOSE curDBs
	DEALLOCATE curDBs

	-- =============================================================================
	-- Full backups
	; WITH cteFullBackups AS (
		SELECT 
			  ROW_NUMBER() OVER(PARTITION BY server_name, database_name ORDER BY backup_finish_date DESC ) AS RowNumber
			, server_name AS ServerName
			, database_name AS DatabaseName
			, ISNULL(CONVERT(VARCHAR(16),backup_finish_date, 121), '') AS LastBackup
			, [user_name] AS UserName
			, CONVERT(BIGINT, backup_size / (1024 * 1024)) AS BackupSizeMB
		FROM msdb.dbo.backupset AS B
		WHERE type = 'D' -- D = Full
	)
	UPDATE #tmpDBs
	SET LastFullBackup = X.LastBackup
		, LastFullBackupSizeMB = X.BackupSizeMB
		, LastFullBackupUser = X.UserName
	FROM cteFullBackups AS X
	WHERE X.DatabaseName = #tmpDBs.DBName
	  AND X.RowNumber = 1
	  AND ServerName = @@ServerName

	-- =============================================================================
	-- Log Backups
	; WITH cteLogBackups AS (
		SELECT 
			  ROW_NUMBER() OVER(PARTITION BY server_name, database_name ORDER BY backup_finish_date DESC ) AS RowNumber
			, server_name AS ServerName
			, database_name AS DatabaseName
			, ISNULL(CONVERT(VARCHAR(16),backup_finish_date, 121), '') AS LastBackup
			, [user_name] AS UserName
			, CONVERT(BIGINT, backup_size / (1024 * 1024)) AS BackupSizeMB
		FROM msdb.dbo.backupset AS B
		WHERE type = 'L' -- L = Log
	)

	UPDATE #tmpDBs
	SET LastLogBackup = X.LastBackup
		, LastLogBackupSizeMB = X.BackupSizeMB
		, LastLogBackupUser = X.UserName
	FROM cteLogBackups AS X
	WHERE X.DatabaseName = #tmpDBs.DBName
	  AND X.RowNumber = 1

	-- =============================================================================
	UPDATE #tmpDBs
	SET LastRestoreDate = X.LastRestoreDate
	FROM (SELECT MAX(Restore_date) AS LastRestoreDate
			, DESTINATION_DATABASE_NAME AS DatabaseName
			FROM msdb.dbo.restorehistory
			GROUP BY DESTINATION_DATABASE_NAME
		  ) AS X
	WHERE X.DatabaseName = #tmpDBs.DBName

	-- =============================================================================

	SELECT 
			  @@SERVERNAME AS ServerName
			, D.name AS DatabaseName
			, S.name AS OwnerName
			--, ISNULL(CONVERT(varchar(100), TDB.DataFileSizeMB), '') AS DataFileSizeMB
			--, ISNULL(CONVERT(varchar(100), TDB.LogFileSizeMB), '') AS LogFileSizeMB
			, TDB.DataFileSizeMB
			, TDB.LogFileSizeMB
			, CONVERT(DATE, D.create_date) AS CreateDate
			, ISNULL(CONVERT(VARCHAR(16), TDB.LastRestoreDate, 121), '') AS LastRestoreDate 
			, D.compatibility_level AS CompatibilityLevel
			, D.is_read_only AS IsReadOnly
			, D.state_desc AS State
			, D.recovery_model_desc AS RecoveryModel
			
			, ISNULL(CONVERT(VARCHAR(16), TDB.LastFullBackup, 121), '') AS LastFullBackup
			, ISNULL(CONVERT(VARCHAR(50), TDB.LastFullBackupSizeMB), '') AS LastFullBackupSizeMB
			, ISNULL(TDB.LastFullBackupUser, '') AS LastFullBackupUser
			, ISNULL(CONVERT(VARCHAR(16), TDB.LastLogBackup, 121), '') AS LastLogBackup
			, ISNULL(CONVERT(VARCHAR(50), TDB.LastLogBackupSizeMB), '') AS LastLogBackupSizeMB
			, ISNULL(TDB.LastLogBackupUser, '') AS LastLogBackupUser

			, ISNULL(TDB.Business_Owner, '') AS Business_Owner
			, ISNULL(TDB.Business_Description, '') AS Business_Description
			, CASE WHEN database_id <=4 THEN 1 ELSE 0 END AS IsSystemDatabase
	FROM sys.databases AS D
	LEFT OUTER JOIN #tmpDBs AS TDB ON D.name = TDB.DBName
	LEFT OUTER JOIN sys.syslogins AS S ON S.sid = D.owner_sid

	ORDER BY IsSystemDatabase, D.name

	IF OBJECT_ID('tempdb..#tmpDBs') IS NOT NULL
	BEGIN
		DROP TABLE #tmpDBs
	END

END