-- Last_Exec_time_for_a_stored_procedure
-- 2022-08-14 @bdill (stolen from somewhere online and modified)

USE MyDatabase

SELECT p.object_id
     --, d.database_id
     , DB_NAME(d.database_id)                   AS DatabaseName
     --, S.name                                   AS SchemaName
     --, p.name                                   AS SPName
	 , S.name + '.' + p.name					AS SPName
     , d.last_execution_time
	 , DATEDIFF(s, d.last_execution_time, GETDATE()) AS LastExecSecondsAgo
     , d.cached_time
     , ISNULL(d.execution_count, 0)             AS ExecCount
     , d.total_elapsed_time
     , d.total_elapsed_time / d.execution_count AS avg_elapsed_time
     , d.last_elapsed_time
     , p.create_date                            
     , p.modify_date                            
FROM sys.procedures                   AS p
LEFT JOIN sys.dm_exec_procedure_stats AS d ON d.object_id = p.object_id
LEFT OUTER JOIN sys.schemas           AS S ON S.schema_id = p.schema_id

WHERE p.is_ms_shipped = 0
--AND DATEADD(MINUTE, -60, GETDATE()) < d.last_execution_time -- ran in last 60 minutes

ORDER BY d.last_execution_time DESC;