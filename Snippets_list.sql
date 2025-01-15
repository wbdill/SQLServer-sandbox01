/*
================================================================================
Make the most out of your snippet manager.

Red-Gate SQL Prompt ($210/yr subscr only) - https://www.red-gate.com/products/sql-prompt/
Devart SQL Complete ($250/$300/$400 perpetual) - https://www.devart.com/dbforge/sql/sqlcomplete/ordering.html
SSMS Tools Pack (~$40 per machine) - https://www.ssmstoolspack.com/Licensing

================================================================================
<snippet abbreviation> - <description>
<Actual snippet>

{C} indicates "cursor here" if your snippet manager/utility supports it.
================================================================================
*/


-- ================================================================================
-- Short and sweet snippets
-- ================================================================================

-- SSF
SELECT * FROM 

-- STT
SELECT TOP(100) * FROM 

-- SCGB - select count group by
SELECT StatusID, COUNT(*) AS N FROM Orders GROUP BY StatusID ORDER BY StatusID

-- SCGBH - select count group by having
SELECT StatusID, COUNT(*) AS N FROM Orders GROUP BY StatusID HAVING COUNT(*) > 0 ORDER BY StatusID

-- lc - line comment
-- ================================================================================

-- lcl - line comment light
-- --------------------------------------------------------------------------------

-- /* - multi-line comment
/*
{C}
*/



-- ================================================================================
-- Longer snippets
-- ================================================================================
-- scol - select/search by column name, table name
SELECT S.name AS SchemaName, T.name AS TableName, C.name AS ColumnName
, CASE WHEN Y.name IN ('char', 'nchar', 'varchar', 'nvarchar') THEN CONCAT(UPPER(Y.name), '(', REPLACE(C.max_length, -1, 'MAX'), ')') 
       WHEN Y.name IN ('decimal') THEN CONCAT(UPPER(Y.name), '(', C.PRECISION, ', ', C.scale, ')') 
  ELSE UPPER(Y.name) END AS DataType
, CASE WHEN C.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END AS Nullable
FROM sys.schemas AS S
JOIN sys.tables AS T ON S.SCHEMA_ID = T.SCHEMA_ID
JOIN sys.columns AS C ON T.OBJECT_ID = C.OBJECT_ID
JOIN sys.types AS Y ON Y.user_type_id = C.user_type_id
WHERE 1=1
AND S.name LIKE '%%'
AND T.name LIKE '%%' --<< Enter table name to search on
AND C.name LIKE '%{C}%' --<< Enter column name to search on
ORDER BY S.name, T.name, C.column_id;

-- ================================================================================
-- ccur - create cursor
SET NOCOUNT ON;
DECLARE {C}@ID INT, @LastName NVARCHAR(50);

DECLARE cur CURSOR FAST_FORWARD FOR
	SELECT 
	FROM 
	WHERE 
	ORDER BY ;

OPEN cur
FETCH NEXT FROM cur INTO @ID, @LastName
WHILE @@FETCH_STATUS = 0
BEGIN
	-- Do stuff
	SELECT @ID AS ID, @LastName AS LastName    

END
CLOSE cur;
DEALLOCATE cur;