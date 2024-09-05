-- =======================================================================================
-- Script:  adm_Missing_FK_analysis
-- Desc:    Find columns that should be a Foreign Key, but aren't.  
--          Assumes a consistent naming of PK and FK cols.  Ex: All candidate cols end with "ID"
-- License: Shoutoutware.  If this script helped you out, give me a shout out on Twitter: @bdill and reference adm_Missing_FK_analysis
--          Also msg me if you find errors or have suggestions.
-- Auth:    W. Brian Dill (@bdill on Twitter) 
-- Date:    2022-05-15  intial query w/ some queries from the web (noted in comments)
-- Script home: https://github.com/wbdill/SQLServer-sandbox01/blob/master/adm_Missing_FK_analysis.sql
-- Other useful scripts: https://github.com/wbdill/SQLServer-sandbox01
-- =======================================================================================
-- FK Reference:  https://docs.microsoft.com/en-us/sql/relational-databases/tables/create-foreign-key-relationships?view=sql-server-ver15

DROP TABLE IF EXISTS dbo.ztmp_FK_analysis_bdill
;WITH Cand AS (
   SELECT S.name                                AS Candidate_Schema
        , T.name                                AS Candidate_Table
        , C.name                                AS Candidate_Column
        , S.name + '.' + T.name + '.' + C.name  AS Candidate_Full
	FROM sys.schemas AS S
	JOIN sys.tables  AS T ON T.schema_id = S.schema_id
	JOIN sys.columns AS C ON C.object_id = T.object_id

	WHERE (C.name LIKE '%ID' OR C.name LIKE '%SignedBy')  -- << Enter desired criteria for what might make a candidate column
	AND S.name NOT IN ('srim')                            -- << Enter desired criteria for what might make a candidate column
	AND T.name NOT IN ('tmpUsers', 'sysdiagrams')         -- << Enter desired criteria for what might make a candidate column
	AND T.name NOT LIKE 'ztmp_%'                          -- << Enter desired criteria for what might make a candidate column
	AND T.name NOT LIKE 'tmp_%'                           -- << Enter desired criteria for what might make a candidate column
) 
, FKs AS (  -- https://dataedo.com/kb/query/sql-server/list-foreign-keys-sql-queries
	SELECT SCHEMA_NAME(fk_tab.schema_id) + '.' + fk_tab.name + '.'+ fk_col.name AS FK_Full
		 , fk_tab.name                                                          AS FK_Table
		 , fk_col.name                                                          AS FK_Column
		 , '>-'	                                                                AS rel
		 , SCHEMA_NAME(pk_tab.schema_id) + '.' + pk_tab.name                    AS FK_PK_Table
		 , pk_col.name                                                          AS FK_PK_Column
		 , fk.name                                                              AS FK_Name
		 , fk_cols.constraint_column_id                                         AS FK_colid
	FROM sys.foreign_keys              AS fk
	INNER JOIN sys.tables              AS fk_tab ON fk_tab.object_id = fk.parent_object_id
	INNER JOIN sys.tables              AS pk_tab ON pk_tab.object_id = fk.referenced_object_id
	INNER JOIN sys.foreign_key_columns AS fk_cols ON fk_cols.constraint_object_id = fk.object_id
	INNER JOIN sys.columns             AS fk_col ON fk_col.column_id = fk_cols.parent_column_id AND fk_col.object_id = fk_tab.object_id
	INNER JOIN sys.columns             AS pk_col ON pk_col.column_id = fk_cols.referenced_column_id AND pk_col.object_id = pk_tab.object_id
)
, PKs AS (  -- https://dataedo.com/kb/query/sql-server/list-all-primary-keys-and-their-columns
	SELECT SCHEMA_NAME(tab.schema_id) + '.' + tab.name + '.' + col.name AS PK_Full
		 , SCHEMA_NAME(tab.schema_id) AS PK_Schema
		 , tab.[name]                 AS PK_Table
		 , col.[name]                 AS PK_Column
		 , pk.[name]                  AS PK_Name
		 , ic.index_column_id         AS PK_colid
	FROM sys.tables              AS tab
	INNER JOIN sys.indexes       AS pk ON tab.object_id = pk.object_id AND pk.is_primary_key = 1
	INNER JOIN sys.index_columns AS ic ON ic.object_id = pk.object_id AND ic.index_id = pk.index_id
	INNER JOIN sys.columns       AS col ON pk.object_id = col.object_id AND col.column_id = ic.column_id
)
-- =======================================================================================
-- Query the 3 CTEs to populate ztmp_FK_analysis_bdill
SELECT Cand.Candidate_Full
     , Cand.Candidate_Schema
     , Cand.Candidate_Table
     , Cand.Candidate_Column
     , CASE WHEN PKs.PK_Full IS NULL THEN 0 ELSE 1 END AS IsPK
     , CASE WHEN FKs.FK_Full IS NULL THEN 0 ELSE 1 END AS HasFK
     , FKs.FK_Full
     , FKs.FK_Table
     , FKs.FK_Column
     , FKs.rel
     , FKs.FK_PK_Table
     , FKs.FK_PK_Column
     , FKs.FK_Name
     , FKs.FK_colid
     , PKs.PK_Full
     , PKs.PK_Schema
     , PKs.PK_Table
     , PKs.PK_Column
     , PKs.PK_Name
     , PKs.PK_colid 
INTO dbo.ztmp_FK_analysis_bdill
FROM Cand
LEFT OUTER JOIN FKs ON Cand.Candidate_Full = FKs.FK_Full
LEFT OUTER JOIN PKs ON Cand.Candidate_Full = PKs.PK_Full

-- =======================================================================================
-- Query 0) Entire table
-- SELECT * FROM dbo.ztmp_FK_analysis_bdill

-- =======================================================================================
-- Query 1) Tables with Composite Primary Keys (CPK).  Look at each of the cols to see if they need to have a FK.
SELECT 'Query 1 - Tables with CPKs' AS Info
, PK_Schema, PK_Table, STRING_AGG(PK_Column, ', ') AS CPK_Cols, PK_Name, COUNT(*) AS NumCols
FROM dbo.ztmp_FK_analysis_bdill 
WHERE PK_Name IS NOT NULL 
GROUP BY PK_Schema, PK_Table, PK_Name HAVING COUNT(*) > 1

-- =======================================================================================
-- Query 2) Candidates that don't have a FK (and are NOT a PK) - i.e. you PROBABLY need to create FK's for these columns
-- You will need to define the correct parent (dbo.__ParentTable__) after "REFERENCES" in the FKScript_Helper
-- If the FK column name and PK column name are not the same, you will also need to modify the column for the __ParentTable__ in the FKScript_Helper
SELECT 'Query 2 - Cols that probably need a FK' AS Info
	, Candidate_Full, Candidate_Table, Candidate_Column
	, 'ALTER TABLE ' + Candidate_Schema + '.' + Candidate_Table + ' ADD CONSTRAINT FK_' + Candidate_Table + '_' + Candidate_Column 
		+ ' FOREIGN KEY (' + Candidate_Column + ') REFERENCES dbo.__ParentTable__ (' + Candidate_Column + ')'
		+ ' ON UPDATE NO ACTION  ON DELETE  NO ACTION; ' AS FKScript_Helper__set_ParentTable_and_schema

FROM dbo.ztmp_FK_analysis_bdill 
WHERE FK_Full IS NULL  -- Candidates that DON'T already have a FK...
AND PK_Full IS NULL    -- ... and are NOT a PK b/c PK's usually aren't also FKs (Except for CPK's - see Query 2)
ORDER BY Candidate_Full

-- =======================================================================================
-- Query 3) Distinct Candidate tables with 1+ Candidate columns without a FK
SELECT 'Query 3 - Cand tables w/ 1+ cand cols.  Like query 2, but 1 row / table' AS Info
	,Candidate_Table, STRING_AGG(Candidate_Column, ', ') AS CandiateCols, COUNT(*) AS NumOfCandidateCol
FROM dbo.ztmp_FK_analysis_bdill 
WHERE FK_Full IS NULL
AND PK_Full IS NULL
GROUP BY Candidate_Table ORDER BY Candidate_Table



