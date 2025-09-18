/*
USE MyUtilityDatabase
GO
CREATE TABLE [dbo].[CPUMon](
	[EventTime] [datetime2](2) NOT NULL,
	[InstancePct] [tinyint] NOT NULL,
	[OtherPct] [tinyint] NOT NULL,
	[IdlePct] [tinyint] NOT NULL,
	[EventTimestamp] [bigint] NOT NULL,
 CONSTRAINT [PK_CPUMon_EventTime] PRIMARY KEY CLUSTERED ([EventTime] ASC )
 WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
*/
-- ================================================================================

GO
-- Desc: Poor man's SQL CPU monitor.  
-- Notes: Run at least once every 30 minutes to caputure the CPU usage once per minute from the sys.dm_os_ring_buffers
-- Auth: Brian Dill - briandill.com
-- Created: 

CREATE OR ALTER PROCEDURE dbo.adm_CPUMon_ins
AS
BEGIN
	DECLARE @ts_now BIGINT, @LastEntry datetime
	SELECT @ts_now = cpu_ticks / CONVERT(FLOAT, cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info

	IF EXISTS(SELECT * FROM dbo.CPUMon)
		SELECT @LastEntry = MAX(EventTime) FROM dbo.CPUMon
	ELSE
		SELECT @LastEntry = 0

	INSERT dbo.CPUMon(EventTime, InstancePct, IdlePct, OtherPct, EventTimestamp)
	SELECT --record_id,
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS EventTime, 
			SQLProcessUtilization,
			SystemIdle,
			100 - SystemIdle - SQLProcessUtilization AS OtherProcessUtilization,
			TIMESTAMP
	FROM (SELECT record.value('(./Record/@id)[1]', 'bigint') AS record_id,
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'bigint') AS SystemIdle,
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'bigint') AS SQLProcessUtilization,
				TIMESTAMP
			FROM (SELECT TIMESTAMP, CONVERT(XML, record) AS record 
					FROM sys.dm_os_ring_buffers 
					WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
					AND record LIKE '% %'
				) AS x
		) AS y 
	WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > @LastEntry
	AND y.SQLProcessUtilization IS NOT NULL
END
GO
