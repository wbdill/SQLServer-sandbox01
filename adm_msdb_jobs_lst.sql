/*
** Desc: Selects the jobs/steps on a SQL 2005+ box.
** Auth: Brian Dill
** Date: 2015-11-09 (2000 version: 2007-01-26)
** Date: 2025-04-07 - now repeating JobName with step_id in parenthesis for multi-step jobs
** Date: 2025-04-15 - StepRunLengthSec now corrected using Tony's modulo method
** Date: 2025-05-08 - Added repeat, StartingAt, EndingAt from https://www.mssqltips.com/sqlservertip/5019/sql-server-agent-job-schedule-reporting/
** Date: 2025-05-08 - Added Monthly Relative from https://www.sqlservercentral.com/articles/how-to-decipher-sysschedules 
*/
USE master
GO
CREATE OR ALTER PROCEDURE dbo.adm_msdb_jobs_lst 
	 @ShowDisabledJobs BIT = 1
AS
BEGIN
	SELECT GETDATE() AS [Timestamp]
		, Serv.srvname AS Servername
		, CASE SJS.step_id 
				WHEN 1 THEN SJ.name 
	 			ELSE CONCAT(SJ.name, ' (', SJS.step_id, ')')
			 END AS JobName
		, CASE WHEN SJS.step_id = 1 THEN
			CASE SJ.description WHEN 'No description available.' THEN '' ELSE ISNULL(SJ.description, '') 
			END 
		  ELSE ''
		  END AS 'JobDescription'		
		, SJ.enabled
		, CASE WHEN SJS.step_id = 1 THEN
				SS.name
				ELSE ''
		  END AS schedule_name
		, CASE SJS.step_id 
			WHEN 1 THEN 
				CASE SS.freq_type 
					WHEN 1 THEN 'Once' 
					WHEN 4 THEN 'Daily' 
					WHEN 8 THEN 'Weekly' 
					WHEN 16 THEN 'Monthly' 
					WHEN 32 THEN 'Monthly Relative'
					WHEN 64 THEN 'On SQL Agent start'
					ELSE '' 
				END 
			ELSE '' 
			END AS Frequency	
		
		, CASE WHEN SJS.step_id = 1 THEN
				CASE freq_type 
					WHEN 1 THEN LEFT(CONVERT(VARCHAR, active_start_date), 4) + '-' + SUBSTRING(CONVERT(VARCHAR, active_start_date), 5, 2) + '-' + RIGHT(CONVERT(VARCHAR, active_start_date), 2)
					WHEN 4 THEN
						'every ' + cast (freq_interval as varchar(3)) + ' day(s)'
					WHEN 8 THEN 
								  CASE WHEN freq_interval&1 = 1 THEN 'Sun' ELSE '' END
								+ CASE WHEN freq_interval&2 = 2 THEN 'Mon' ELSE '' END
								+ CASE WHEN freq_interval&4 = 4 THEN 'Tue' ELSE '' END
								+ CASE WHEN freq_interval&8 = 8 THEN 'Wed' ELSE '' END
								+ CASE WHEN freq_interval&16 = 16 THEN 'Thu' ELSE '' END
								+ CASE WHEN freq_interval&32 = 32 THEN 'Fri' ELSE '' END
								+ CASE WHEN freq_interval&64 = 64 THEN 'Sat' ELSE '' END
					WHEN 16 THEN CONVERT(varchar, freq_interval) -- 16 = Monthly
					WHEN 32 THEN -- 32 = Monthly relative
						CASE SS.freq_relative_interval
							WHEN 1 THEN 'First '
							WHEN 2 THEN 'Second '
							WHEN 4 THEN 'Third '
							WHEN 8 THEN 'Fourth '
							WHEN 16 THEN 'Last '
						END
						+ CASE SS.freq_interval
							WHEN 1 THEN 'Sunday'
							WHEN 2 THEN 'Monday'
							WHEN 3 THEN 'Tuesday'
							WHEN 4 THEN 'Wednesday'
							WHEN 5 THEN 'Thursday'
							WHEN 6 THEN 'Friday'
							WHEN 7 THEN 'Saturday'
							WHEN 8 THEN 'Day'
							WHEN 9 THEN 'Weekday'
							WHEN 10 THEN 'Weekend Day'
							END
						+ ' of every ' + CONVERT(VARCHAR(50), SS.freq_recurrence_factor) + ' month(s)'
					WHEN 64 THEN 'on SQLAgent start'	-- 64 = when SQL Server Agent starts
					ELSE 'Other'
				END 
			ELSE ''
			END AS FreqInterval
		--, SS.freq_recurrence_factor

		, CASE WHEN SJS.step_id = 1 THEN 
			 CASE 
				WHEN freq_subday_type = 2 then ' every ' + cast(freq_subday_interval as varchar(7)) + ' seconds' 
				WHEN freq_subday_type = 4 then ' every ' + cast(freq_subday_interval as varchar(7)) + ' minutes' 
				WHEN freq_subday_type = 8 then ' every ' + cast(freq_subday_interval as varchar(7)) + ' hours'  
				ELSE '' 
			 END
		   ELSE '' END AS [Repeat]
		 
		 , CASE WHEN SJS.step_id = 1 THEN  
			 CASE 
				WHEN freq_subday_type = 2 then stuff(stuff(RIGHT(replicate('0', 6) +  cast(active_start_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':')
				WHEN freq_subday_type = 4 then stuff(stuff(RIGHT(replicate('0', 6) +  cast(active_start_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':')
				WHEN freq_subday_type = 8 then stuff(stuff(RIGHT(replicate('0', 6) +  cast(active_start_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':')
				ELSE stuff(stuff(RIGHT(replicate('0', 6) +  cast(active_start_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':')
				END
			ELSE '' END AS StartingAt
		, CASE WHEN SJS.step_id = 1 THEN 
				stuff(stuff(RIGHT(replicate('0', 6) +  cast(active_end_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':') 
			ELSE ''
		  END AS EndingAt

		, CASE WHEN SJS.step_id = 1  AND SJ.enabled = 1 THEN 
			  convert(datetime, 
					left(convert(varchar, SJS.last_run_date), 4) 
			+ '-' + substring(convert(varchar, SJS.last_run_date), 5, 2) 
			+ '-' + substring(convert(varchar, SJS.last_run_date), 7, 2)
			+ 'T' + CASE LEN(convert(varchar, SJS.last_run_time)) WHEN 6 THEN left(convert(varchar, SJS.last_run_time), 2) WHEN 5 THEN '0' + left(convert(varchar, SJS.last_run_time), 1) ELSE '00'  END
			+ ':' + CASE LEN(convert(varchar, SJS.last_run_time)) WHEN 6 THEN substring(convert(varchar, SJS.last_run_time), 3,2) WHEN 5 THEN substring(convert(varchar, SJS.last_run_time), 2, 2) WHEN 4 THEN substring(convert(varchar, SJS.last_run_time), 1, 2) WHEN 3 THEN  '0' + substring(convert(varchar, SJS.last_run_time), 1, 1) END
			+ ':' + RIGHT(convert(varchar, SJS.last_run_time), 2)
			)
			ELSE NULL
			END AS LastRunDate
		, CASE WHEN SJS.step_id = 1  AND SJ.enabled = 1 THEN 
			  convert(datetime, 
					left(convert(varchar, SJSched.next_run_date), 4) 
			+ '-' + substring(convert(varchar, SJSched.next_run_date), 5, 2) 
			+ '-' + substring(convert(varchar, SJSched.next_run_date), 7, 2)
			+ 'T' + CASE LEN(convert(varchar, SJSched.next_run_time)) WHEN 6 THEN left(convert(varchar, SJSched.next_run_time), 2) WHEN 5 THEN '0' + left(convert(varchar, SJSched.next_run_time), 1) ELSE '00'  END
			+ ':' + CASE LEN(convert(varchar, SJSched.next_run_time)) WHEN 6 THEN substring(convert(varchar, SJSched.next_run_time), 3,2) WHEN 5 THEN substring(convert(varchar, SJSched.next_run_time), 2, 2) WHEN 4 THEN substring(convert(varchar, SJS.last_run_time), 1, 2) WHEN 3 THEN  '0' + substring(convert(varchar, SJS.last_run_time), 1, 1) END
			+ ':' + RIGHT(convert(varchar, SJSched.next_run_time), 2)
			)
			ELSE NULL
		  END AS NextRunDate
		, SJS.step_id
		, SJS.step_name
		, ISNULL(SJS.database_name, ' ') AS 'DBName'
		, (SJS.last_run_duration % 100) -- seconds 
			+ (SJS.last_run_duration % 10000 / 100 * 60) -- minutes (integer division / 100 chops off the seconds)
			+ (SJS.last_run_duration / 10000 * 3600) -- hours 
			AS StepRunLengthSec
		, CASE 
			WHEN (SJS.last_run_outcome = 0 AND SJS.last_run_date > 0) THEN 'Failed' 
			WHEN SJS.last_run_outcome = 1 THEN 'Success' 
			WHEN SJS.last_run_outcome = 3 THEN 'Cancelled' 
			WHEN SJS.last_run_outcome = 5 THEN 'Unknown' 
			ELSE '' END AS LastRunOutcome
		, SJS.subsystem
		, SJS.command
	    
	FROM msdb.dbo.sysjobs AS SJ
	LEFT OUTER JOIN msdb.dbo.sysjobschedules AS SJSched on SJ.job_id = SJSched.job_id
	LEFT OUTER JOIN msdb.dbo.sysschedules AS SS on SJSched.schedule_id = SS.schedule_id
	LEFT OUTER JOIN msdb.dbo.sysjobsteps AS SJS ON SJS.job_id = SJ.job_id
	LEFT OUTER JOIN msdb.sys.sysservers AS Serv ON Serv.srvid = SJ.originating_server_id
	WHERE (SJ.enabled = 1 OR @ShowDisabledJobs = 1)
	ORDER BY SJ.name, SJ.job_id, SJSched.schedule_id, SJS.step_id
END
GO