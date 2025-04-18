/*
** Desc: Selects the jobs/steps on a SQL 2005+ box.
** Auth: Brian Dill
** Date: 2015-11-09 (2000 version: 2007-01-26)
** Date: 2025-04-07 - now repeating JobName with step_id in parenthesis for multi-step jobs
** Date: 2025-04-15 - StepRunLengthSec now corrected using Tony's modulo method
*/
ALTER PROCEDURE [dbo].[adm_msdb_jobs_lst]

	 @ShowDisabledJobs BIT = 1

AS

SELECT 
	 --SJ.job_id,
	 --SJ.originating_server_id AS 'Server'
	 Serv.srvname AS 'ServerName'
	,CASE SJS.step_id 
		WHEN 1 THEN SJ.name 
		ELSE CONCAT(SJ.name, ' (', SJS.step_id, ')')
	 END AS 'JobName'

	,SJ.enabled

	,CASE SJS.step_id 
		WHEN 1 THEN CASE SS.freq_type WHEN 1 THEN 'Once' WHEN 4 THEN 'Daily' WHEN 8 THEN 'Weekly' WHEN 16 THEN 'Monthly' ELSE '' END 
		ELSE '' 
	 END AS Frequency

	,CASE SJS.step_id WHEN 1 THEN 
		 CASE SS.freq_type  
			-- 1 = Once
			WHEN 1 THEN LEFT(CONVERT(VARCHAR, active_start_date), 4) + '-' + SUBSTRING(CONVERT(VARCHAR, active_start_date), 5, 2) + '-' + RIGHT(CONVERT(VARCHAR, active_start_date), 2)
			WHEN 4 THEN 'Daily' -- 4 = Daily
			WHEN 8 THEN 		-- 8 = weekly
				CASE WHEN SS.freq_interval & 1 = 1 THEN 'Sun' ELSE '' END 
				 + CASE WHEN SS.freq_interval & 2 = 2 THEN 'Mon' ELSE '' END
				 + CASE WHEN SS.freq_interval & 4 = 4 THEN 'Tue' ELSE '' END
				 + CASE WHEN SS.freq_interval & 8 = 8 THEN 'Wed' ELSE '' END
				 + CASE WHEN SS.freq_interval & 16 = 16 THEN 'Thu' ELSE '' END
				 + CASE WHEN SS.freq_interval & 32 = 32 THEN 'Fri' ELSE '' END
				 + CASE WHEN SS.freq_interval & 64 = 64 THEN 'Sat' ELSE '' END			
			WHEN 16 THEN CONVERT(varchar, freq_interval) -- 16 = Monthly
			WHEN 32 THEN 'Monthly relative'		-- 32 = Monthly relative
			WHEN 64 THEN 'on SQLAgent start'	-- 64 = when SQL Server Agent starts
			ELSE 'Other' END
	 ELSE '' END AS 'FreqInterval'

	,CASE SJS.step_id WHEN 1 THEN 
			ISNULL(
					LEFT(REPLICATE('0', 6-LEN(SS.active_start_time)) + CONVERT(varchar, SS.active_start_time), 2)
					 + ':' + SUBSTRING(REPLICATE('0', 6-LEN(SS.active_start_time)) + CONVERT(varchar, SS.active_start_time), 3, 2)
					 + ':' + RIGHT(REPLICATE('0', 6-LEN(SS.active_start_time)) + CONVERT(varchar, SS.active_start_time), 2)
			 , '')
	 ELSE '' END AS 'StartTime'

	,CASE SJ.description WHEN 'No description available.' THEN '' ELSE ISNULL(SJ.description, '') END AS 'JobDescription'
--	,SJ.date_modified
	,SJS.step_id
	,SJS.step_name
	--,SJS.last_run_duration AS 'StepRunLengthSec'
	,(SJS.last_run_duration%100) -- seconds 
			+ (SJS.last_run_duration%10000 / 100 * 60) -- minutes (integer division / 100 chops off the seconds)
			+ (SJS.last_run_duration/10000 * 3600) -- hours 
			AS StepRunLengthSec
	,SJS.subsystem
	,SJS.command
	,ISNULL(SJS.database_name, ' ') AS 'DBName'
--	,SJS.last_run_date
--	,SJS.last_run_time
	,CASE SJ.enabled
		WHEN 1 THEN convert(datetime, 
				left(convert(varchar, SJS.last_run_date), 4) 
		+ '-' + substring(convert(varchar, SJS.last_run_date), 5, 2) 
		+ '-' + substring(convert(varchar, SJS.last_run_date), 7, 2)
		+ 'T' + CASE LEN(convert(varchar, SJS.last_run_time)) WHEN 6 THEN left(convert(varchar, SJS.last_run_time), 2) WHEN 5 THEN '0' + left(convert(varchar, SJS.last_run_time), 1) ELSE '00'  END
		+ ':' + CASE LEN(convert(varchar, SJS.last_run_time)) WHEN 6 THEN substring(convert(varchar, SJS.last_run_time), 3,2) WHEN 5 THEN substring(convert(varchar, SJS.last_run_time), 2, 2) WHEN 4 THEN substring(convert(varchar, SJS.last_run_time), 1, 2) WHEN 3 THEN  '0' + substring(convert(varchar, SJS.last_run_time), 1, 1) END
		+ ':' + RIGHT(convert(varchar, SJS.last_run_time), 2)
		)
		ELSE NULL
	  END AS LastRunDate
	,CASE SJ.enabled
		WHEN 1 THEN convert(datetime, 
				left(convert(varchar, SJSched.next_run_date), 4) 
		+ '-' + substring(convert(varchar, SJSched.next_run_date), 5, 2) 
		+ '-' + substring(convert(varchar, SJSched.next_run_date), 7, 2)
		+ 'T' + CASE LEN(convert(varchar, SJSched.next_run_time)) WHEN 6 THEN left(convert(varchar, SJSched.next_run_time), 2) WHEN 5 THEN '0' + left(convert(varchar, SJSched.next_run_time), 1) ELSE '00'  END
		+ ':' + CASE LEN(convert(varchar, SJSched.next_run_time)) WHEN 6 THEN substring(convert(varchar, SJSched.next_run_time), 3,2) WHEN 5 THEN substring(convert(varchar, SJSched.next_run_time), 2, 2) WHEN 4 THEN substring(convert(varchar, SJS.last_run_time), 1, 2) WHEN 3 THEN  '0' + substring(convert(varchar, SJS.last_run_time), 1, 1) END
		+ ':' + RIGHT(convert(varchar, SJSched.next_run_time), 2)
		)
		ELSE NULL
	  END AS NextRunDate
	, CASE 
		WHEN (SJS.last_run_outcome = 0 AND SJS.last_run_date > 0) THEN 'Failed' 
		WHEN SJS.last_run_outcome = 1 THEN 'Success' 
		WHEN SJS.last_run_outcome = 3 THEN 'Cancelled' 
		WHEN SJS.last_run_outcome = 5 THEN 'Unknown' 
		ELSE '' END AS LastRunOutcome
	--, SJS.last_run_outcome
	--, SJS.last_run_date
	--, SJSched.next_run_date
	--, SJSched.next_run_time

FROM msdb.dbo.sysjobs AS SJ
LEFT OUTER JOIN msdb.dbo.sysjobsteps AS SJS ON SJS.job_id = SJ.job_id
LEFT OUTER JOIN msdb.dbo.sysjobschedules AS SJSched ON SJSched.job_id = SJ.job_id
LEFT OUTER JOIN msdb.dbo.sysschedules AS SS ON SS.schedule_id = SJSched.schedule_id
LEFT OUTER JOIN msdb.sys.sysservers AS Serv ON Serv.srvid = SJ.originating_server_id

WHERE (SJ.enabled = 1 OR @ShowDisabledJobs = 1)

ORDER BY SJ.name, SJ.job_id, SJSched.schedule_id, SJS.step_id

