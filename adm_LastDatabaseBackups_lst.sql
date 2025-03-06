-- =============================================================================
-- Database backup info
-- Auth: bdill (W. Brian Dill) @bdill
-- Date: 2013-11-20
-- =============================================================================
CREATE OR ALTER PROCEDURE [dbo].[adm_LastDatabaseBackups_lst]

AS
BEGIN
	SET NOCOUNT ON;

	-- =============================================================================
	-- Business_Owner and Business_Description are extended properties to self-document the DBs
	-- =============================================================================
	IF OBJECT_ID('tempdb..#tmpDBs') IS NULL
	BEGIN
		CREATE TABLE #tmpDBs ( DBName varchar(1000) NOT NULL, Business_Owner VARCHAR(100) NULL, Business_Description VARCHAR(1000) NULL )
		INSERT INTO #tmpDBs ( DBName ) SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name
	END
	DECLARE @dbname VARCHAR(1000) 
	DECLARE @sql NVARCHAR(max)
	DECLARE @tmpOwner VARCHAR(100)
	DECLARE @tmpDesc VARCHAR(1000)

	DECLARE curDBs CURSOR LOCAL FORWARD_ONLY FOR 
		SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE' ORDER BY name
	OPEN curDBs
	FETCH NEXT FROM curDBs INTO @dbname
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @sql = N'SELECT @tmpOwner = CONVERT(VARCHAR(100), value) FROM ' + @dbname + '.sys.extended_properties WHERE class_desc = ''DATABASE'' AND name = ''Business_Owner'' '
		EXEC sp_executesql @statement = @sql, @params = N'@tmpOwner varchar(100) OUTPUT', @tmpOwner = @tmpOwner OUTPUT

		SET @sql = N'SET @tmpDesc  = (SELECT CONVERT(VARCHAR(1000), value) FROM ' + @dbname + '.sys.extended_properties WHERE class_desc = ''DATABASE'' AND name = ''Business_Description'' )'
		EXEC sp_executesql @statement = @sql, @params = N'@tmpDesc varchar(1000) OUTPUT', @tmpDesc = @tmpDesc OUTPUT

		SET @tmpDesc  = ( SELECT ISNULL(REPLACE(@tmpDesc, '''', ''''''), '') )
		SET @tmpOwner = ( SELECT ISNULL(REPLACE(@tmpOwner, '''', ''''''), '') )

		SET @sql = N'UPDATE #tmpDBs SET Business_Owner = ''' + @tmpOwner + ''' , Business_Description = ''' + @tmpDesc + ''' WHERE DBName = ''' + @dbname + ''''
		EXEC sys.sp_executesql @statement = @sql
		SET @tmpDesc = ''
		SET @tmpOwner = ''
	
		FETCH NEXT FROM curDBs INTO @dbname
	END
	CLOSE curDBs
	DEALLOCATE curDBs

	--SELECT * FROM #tmpDBs AS TDB ORDER BY DBName

	-- =============================================================================

	; WITH Backups AS (
		SELECT 
			  ROW_NUMBER() OVER(PARTITION BY server_name, database_name ORDER BY backup_finish_date DESC ) AS RowNumber
			, server_name AS ServerName
			, database_name AS DatabaseName
			, CONVERT(VARCHAR(16), database_creation_date, 121) AS DatabaseCreateDate
			, ISNULL(CONVERT(VARCHAR(16),backup_finish_date, 121), '') AS LastBackup
			, CASE type
				WHEN 'D' THEN 'Full Database'
				WHEN 'I' THEN 'Diff. Database'
				WHEN 'L' THEN 'Log'
				WHEN 'F' THEN 'File filegroup'
				WHEN 'G' THEN 'Diff. file'
				WHEN 'P' THEN 'Partial'
				WHEN 'Q' THEN 'Diff. partial'
				ELSE '' END AS BackupType
			, [user_name] AS UserName
			, CONVERT(INT, backup_size / (1024 * 1024)) AS BackupSizeMB
			, backup_set_id
		FROM msdb.dbo.backupset AS B
	)

	SELECT 
			  @@SERVERNAME AS ServerName
			, D.name AS DatabaseName
			, S.name AS OwnerName
			, CONVERT(DATE, D.create_date) AS CreateDate
			, D.compatibility_level AS CompatibilityLevel
			, D.is_read_only AS IsReadOnly
			, D.state_desc AS State
			, D.recovery_model_desc AS RecoveryModel
			, ISNULL(B.LastBackup, '') AS LastBackup
			, ISNULL(B.BackupType, '') AS BackupType
			, ISNULL(B.UserName, '') AS BackupUser
			, ISNULL(CONVERT(VARCHAR(50), B.BackupSizeMB), '') AS BackupSizeMB
			, ISNULL(TDB.Business_Owner, '') AS Business_Owner
			, ISNULL(TDB.Business_Description, '') AS Business_Description
	FROM sys.databases AS D
	LEFT OUTER JOIN #tmpDBs AS TDB ON D.name = TDB.DBName
	LEFT OUTER JOIN Backups AS B ON D.name = B.DatabaseName AND @@SERVERNAME = B.ServerName
	LEFT OUTER JOIN sys.syslogins AS S ON S.sid = D.owner_sid
	WHERE (B.RowNumber = 1 OR B.RowNumber IS NULL)
	  AND D.database_id > 4 -- exclude system DBs
	ORDER BY D.name

	IF OBJECT_ID('tempdb..#tmpDBs') IS NOT NULL
	BEGIN
		DROP TABLE #tmpDBs
	END

END

GO


