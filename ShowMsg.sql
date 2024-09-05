SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- Desc: ISO-date-time - @message.  Uses RAISERROR b/c PRINT can take longer to bubble up.
-- 2022-07-01 @bdill - initial creation
CREATE OR ALTER PROCEDURE dbo.ShowMsg
  @Msg VARCHAR(1000)
AS
BEGIN
	DECLARE @msg2 VARCHAR(1200)
	SET @msg2 = CONVERT(VARCHAR(19), GETDATE(),121) + ' - ' + @msg
	RAISERROR(@msg2, 10, 1) WITH NOWAIT
END
GO

