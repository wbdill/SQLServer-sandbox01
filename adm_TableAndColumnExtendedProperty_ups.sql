-- Desc: 		Inserts/Updates extended properties for a table or column.  Set @ColumnName to NULL or '' to upsert a table.
-- License: 	ShoutOutWare - give me a shout out on Twitter @bdill if this script helped you. :)  (retain comments and do not redistribute)
-- Created: 	2021-03-15 Brian Dill - initial creation
-- Script home: https://github.com/wbdill/SQLServer-sandbox01/adm_TableAndColumnExtendedProperty_ups.sql
-- Other useful files: https://github.com/wbdill/SQLServer-sandbox01
CREATE OR ALTER PROCEDURE dbo.adm_TableAndColumnExtendedProperty_ups
	  @SchemaName sysname
	, @TableName sysname
	, @ColumnName sysname = NULL
	, @Description sysname
	, @PropertyName sysname = 'MS_Description'
AS
BEGIN 
	IF @ColumnName IS NULL OR @ColumnName = ''  -- do Table-level UPSERT
	BEGIN
		IF NOT EXISTS(SELECT * FROM fn_listextendedproperty (NULL, 'schema', @schemaname, 'table', @TableName, NULL, NULL) WHERE name = @PropertyName)
		BEGIN
			EXEC sys.sp_addextendedproperty 
				  @name = @PropertyName, @value = @Description
				, @level0type = N'SCHEMA', @level0name = @schemaname
				, @level1type = N'TABLE',  @level1name = @TableName;  
		END
		ELSE
		BEGIN
			EXEC sys.sp_updateextendedproperty 
				  @name = @PropertyName, @value = @Description
				, @level0type = N'SCHEMA', @level0name = @schemaname
				, @level1type = N'TABLE',  @level1name = @TableName;  
		END
	END
	ELSE										-- Do column-level UPSERT
	BEGIN 
		IF NOT EXISTS(SELECT * FROM fn_listextendedproperty (NULL, 'schema', @schemaname, 'table', @TableName, 'column', @ColumnName) WHERE name = @PropertyName)
		BEGIN
			EXEC sys.sp_addextendedproperty 
				  @name = @PropertyName, @value = @Description
				, @level0type = N'SCHEMA', @level0name = @schemaname
				, @level1type = N'TABLE',  @level1name = @TableName
				, @level2type = N'COLUMN', @level2name = @ColumnName;
		END
		ELSE
		BEGIN
			EXEC sys.sp_updateextendedproperty 
				  @name = @PropertyName, @value = @Description
				, @level0type = N'SCHEMA', @level0name = @schemaname
				, @level1type = N'TABLE',  @level1name = @TableName
				, @level2type = N'COLUMN', @level2name = @ColumnName;
		END
	END 
END 
GO
