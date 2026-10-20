/* =====================================================================
   Tests for study_patch_2026-10-20_text_retention.sql (synthetic data)
   Run as dbo on an EMPTY dev database after the schema and all study
   patches up to and including 2026-10-20 (test_main_stubs.sql before the
   10-17b patch). Every line should start with PASS.

   Participant 1 enrolled 2026-09-15, B3 signed 2026-09-17.
   Participant 2 enrolled 2026-09-15, no B3.
   ===================================================================== */
SET NOCOUNT ON;
GO
CREATE USER app_u WITHOUT LOGIN;   ALTER ROLE study_app ADD MEMBER app_u;
CREATE USER coord_u WITHOUT LOGIN; ALTER ROLE study_coordinator ADD MEMBER coord_u;
CREATE USER deid_u WITHOUT LOGIN;  ALTER ROLE study_deid ADD MEMBER deid_u;
GO
CREATE PROCEDURE dbo.t_check @name NVARCHAR(200), @ok BIT, @detail NVARCHAR(400) = NULL
AS PRINT CONCAT(CASE WHEN @ok = 1 THEN 'PASS ' ELSE 'FAIL ' END, @name,
                CASE WHEN @ok = 1 OR @detail IS NULL THEN '' ELSE ' :: ' + @detail END);
GO
CREATE TYPE dbo.t_deid AS TABLE (Study_Entry_ID BIGINT, Study_Participant_ID INT, Entry_Created_At DATETIME2(0),
                                 Entry_Text NVARCHAR(MAX), Date_Offset_Days SMALLINT);
GO

/* ---------- Setup ---------- */
EXECUTE AS USER = 'coord_u';
DECLARE @ids TABLE (id INT);
INSERT @ids EXEC study.usp_Enroll_Participant N'41-1', N'v1', '2026-09-15T08:00:00', N'T', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
INSERT @ids EXEC study.usp_Enroll_Participant N'41-2', N'v1', '2026-09-15T08:00:00', N'T', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
EXEC study.usp_Record_Text_Retention 1, '2026-09-17T08:00:00', N'B3-v1';
REVERT;
GO

/* ---------- Ingestion ---------- */
DECLARE @a TABLE (s VARCHAR(30));
DECLARE @b TABLE (s VARCHAR(30));
DECLARE @c TABLE (s VARCHAR(30));
EXECUTE AS USER = 'app_u';
INSERT @a EXEC study.usp_Ingest_Entry N'41-1', N'E1', '2026-09-16T20:00:00', N'RAW before B3', 1;   -- entry 1
INSERT @b EXEC study.usp_Ingest_Entry N'41-1', N'E2', '2026-09-18T20:00:00', N'RAW after B3', 1;    -- entry 2
INSERT @c EXEC study.usp_Ingest_Entry N'41-2', N'E3', '2026-09-18T21:00:00', N'RAW no B3', 4;       -- entry 3
REVERT;
DECLARE @ok BIT = CASE WHEN (SELECT s FROM @a) = 'INGESTED_NO_TEXT' AND (SELECT s FROM @b) = 'INGESTED'
                        AND (SELECT s FROM @c) = 'INGESTED_NO_TEXT' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'01 ingestion reports whether the text was kept', @ok;
-- Study_Entry is filtered by row-level security; read it as the coordinator.
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN (SELECT Entry_Text FROM study.Study_Entry WHERE Study_Entry_ID = 1) IS NULL
               AND (SELECT Entry_Text FROM study.Study_Entry WHERE Study_Entry_ID = 2) = N'RAW after B3'
               AND (SELECT Entry_Text FROM study.Study_Entry WHERE Study_Entry_ID = 3) IS NULL THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'02 text is kept only with B3, and only from its signing time', @ok;
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN (SELECT COUNT(*) FROM study.Study_Entry) = 3
               AND (SELECT Is_Restricted FROM study.Study_Entry WHERE Study_Entry_ID = 3) = 1 THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'03 entries without text are still counted, with tier and restriction', @ok;
SET @ok = CASE WHEN (SELECT COUNT(*) FROM study.Access_Audit WHERE Action = 'INGEST' AND Detail = N'no text') = 2 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'04 the audit log records ingestion without text', @ok;
GO

/* ---------- De-identification ---------- */
DECLARE @r dbo.t_deid, @ok BIT = 0, @d NVARCHAR(400);
EXECUTE AS USER = 'deid_u';
INSERT @r EXEC study.usp_Get_Entries_For_Deid;
BEGIN TRY EXEC study.usp_Save_Deid_Text 1, N'DEID', N'test'; END TRY
BEGIN CATCH SET @ok = CASE WHEN ERROR_NUMBER() = 51029 THEN 1 ELSE 0 END; SET @d = ERROR_MESSAGE(); END CATCH;
EXEC study.usp_Save_Deid_Text 2, N'DEID after B3', N'test';
REVERT;
EXEC dbo.t_check N'05 an entry without kept text can''t be de-identified', @ok, @d;
SET @ok = CASE WHEN (SELECT STRING_AGG(CAST(Study_Entry_ID AS VARCHAR(10)), ',') FROM @r) = '2' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'06 the pipeline gets only entries with kept text', @ok;
GO

/* ---------- Revocation ---------- */
EXECUTE AS USER = 'coord_u';
EXEC study.usp_Revoke_Text_Retention 1;
DECLARE @ok BIT = CASE WHEN NOT EXISTS (SELECT 1 FROM study.Study_Entry WHERE Entry_Text IS NOT NULL)
                        AND NOT EXISTS (SELECT 1 FROM study.Entry_Deid)
                        AND (SELECT COUNT(*) FROM study.Study_Entry) = 3 THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'07 revoking B3 erases kept and de-identified text, keeps the entries', @ok;

DECLARE @s TABLE (s VARCHAR(30));
EXECUTE AS USER = 'app_u';
INSERT @s EXEC study.usp_Ingest_Entry N'41-1', N'E4', '2026-09-20T20:00:00', N'RAW after revoke', 1;
REVERT;
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN (SELECT s FROM @s) = 'INGESTED_NO_TEXT'
               AND EXISTS (SELECT 1 FROM study.Study_Entry WHERE Source_Entry_ID = N'E4' AND Entry_Text IS NULL) THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'08 after revoking, new entries are kept without text', @ok;

SET @ok = CASE WHEN EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'pol_study_access' AND is_enabled = 1)
          THEN 1 ELSE 0 END;
EXEC dbo.t_check N'09 the security policy is back on', @ok;
GO
