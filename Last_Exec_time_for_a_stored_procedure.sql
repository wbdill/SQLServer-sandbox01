-- Last_Exec_time_for_a_stored_procedure
-- 2022-08-14 @bdill (stolen from somewhere online and modified)

USE MyDatabase

SELECT P.object_id
	--, D.database_id
	, DB_NAME(d.database_id)					AS DatabaseName
	--, S.name                                   AS SchemaName
	--, p.name                                   AS SPName
	, S.name + '.' + P.name						AS SPName
	, D.last_execution_time
	, DATEDIFF(s, D.last_execution_time, GETDATE()) AS LastExecSecondsAgo
	, D.cached_time
	, ISNULL(D.execution_count, 0)				AS ExecCount
	, D.total_elapsed_time
	, D.total_elapsed_time / d.execution_count	AS avg_elapsed_time
	, D.last_elapsed_time
	, P.create_date                            
	, P.modify_date                            
FROM sys.procedures                   AS P
LEFT JOIN sys.dm_exec_procedure_stats AS D ON D.object_id = P.object_id
LEFT OUTER JOIN sys.schemas           AS S ON S.schema_id = P.schema_id

WHERE P.is_ms_shipped = 0
--AND DATEADD(MINUTE, -60, GETDATE()) < D.last_execution_time -- ran in last 60 minutes

ORDER BY D.last_execution_time DESC;