/* =====================================================================
   Tests for study_patch_2026-10-17_rating.sql (synthetic data only)
   Run as dbo on an EMPTY dev database after:
     study_schema_2026-10-06.sql, study_patch_2026-10-07.sql,
     study_patch_2026-10-17_rating.sql
   Every line should start with PASS. Search the output for FAIL.
   Creates users without logins (app_u, coord_u, deid_u); don't run it
   on a database with real data.

   Entries: 1-3 participant 1 (B3; entry 3 is tier 4, restricted),
            4 participant 2 (no B3), 5 participant 3 (B3).
   Annotators: 1 and 2 raters, 3 adjudicator with restricted access,
               4 rater with no assignments.
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
-- Result shapes of the procedures under test.
CREATE TYPE dbo.t_queue AS TABLE (Study_Entry_ID BIGINT, Study_Participant_ID INT, Has_My_Annotation BIT);
CREATE TYPE dbo.t_entry AS TABLE (Study_Entry_ID BIGINT, Study_Participant_ID INT, Deid_Text NVARCHAR(MAX));
CREATE TYPE dbo.t_adj AS TABLE (Study_Entry_ID BIGINT, Study_Participant_ID INT, Labels INT, Min_Tier TINYINT,
                                Max_Tier TINYINT, Is_Unanimous BIT, Has_Gold BIT);
CREATE TYPE dbo.t_ann AS TABLE (Annotation_ID BIGINT, Annotator_ID INT, Annotator_Type VARCHAR(20), Version_No INT,
                                IsNeglect BIT, IsRepair BIT, IsBid BIT, IsCare BIT, IsShared BIT,
                                Sentiment SMALLINT, Safety_Tier TINYINT, Notes NVARCHAR(2000), Created_At DATETIME2(0));
CREATE TYPE dbo.t_deid AS TABLE (Study_Entry_ID BIGINT, Study_Participant_ID INT, Entry_Created_At DATETIME2(0),
                                 Entry_Text NVARCHAR(MAX), Date_Offset_Days SMALLINT);
GO

/* ---------- Setup ---------- */
EXECUTE AS USER = 'coord_u';
EXEC sp_set_session_context N'actor_user_id', N'200';
DECLARE @h BINARY(32) = HASHBYTES('SHA2_256', 'pdf');
DECLARE @ids TABLE (id INT);
INSERT @ids EXEC study.usp_Register_Agreement N'v1', N'Rae Rater',       '2026-09-01', N'Coord', '2026-09-01', @h, N'TEST', NULL;
INSERT @ids EXEC study.usp_Register_Agreement N'v1', N'Rob Rater',       '2026-09-01', N'Coord', '2026-09-01', @h, N'TEST', NULL;
INSERT @ids EXEC study.usp_Register_Agreement N'v1', N'Ada Adjudicator', '2026-09-01', N'Coord', '2026-09-01', @h, N'TEST', NULL;
INSERT @ids EXEC study.usp_Register_Agreement N'v1', N'Una Unassigned',  '2026-09-01', N'Coord', '2026-09-01', @h, N'TEST', NULL;
INSERT @ids EXEC study.usp_Register_Agreement N'v1', N'Tess Therapist',  '2026-09-01', N'Coord', '2026-09-01', @h, N'TEST', NULL;
INSERT @ids EXEC study.usp_Register_Annotator N'102', 'RATER', 1, 0;
INSERT @ids EXEC study.usp_Register_Annotator N'104', 'RATER', 2, 0;
INSERT @ids EXEC study.usp_Register_Annotator N'103', 'ADJUDICATOR', 3, 1;
INSERT @ids EXEC study.usp_Register_Annotator N'105', 'RATER', 4, 0;
INSERT @ids EXEC study.usp_Enroll_Participant N'41-1', N'TEST-v1', '2026-09-15T08:00:00', N'TEST', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
INSERT @ids EXEC study.usp_Enroll_Participant N'41-2', N'TEST-v1', '2026-09-15T08:00:00', N'TEST', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
INSERT @ids EXEC study.usp_Enroll_Participant N'42-1', N'TEST-v1', '2026-09-15T08:00:00', N'TEST', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
EXEC study.usp_Record_Text_Retention 1, '2026-09-15T08:00:00', N'B3-v1';
EXEC study.usp_Record_Text_Retention 3, '2026-09-15T08:00:00', N'B3-v1';
EXEC study.usp_Assign_Annotator 1, 1; EXEC study.usp_Assign_Annotator 2, 1; EXEC study.usp_Assign_Annotator 3, 1;
EXEC study.usp_Assign_Annotator 1, 2; EXEC study.usp_Assign_Annotator 2, 2; EXEC study.usp_Assign_Annotator 3, 2;
EXEC study.usp_Assign_Annotator 1, 3;
REVERT;
GO
EXECUTE AS USER = 'app_u';
DECLARE @s TABLE (s VARCHAR(30));
INSERT @s EXEC study.usp_Ingest_Entry N'41-1', N'T-E1', '2026-09-16T20:00:00', N'RAW one: Sam called me at work', 1;
INSERT @s EXEC study.usp_Ingest_Entry N'41-1', N'T-E2', '2026-09-17T20:00:00', N'RAW two', 2;
INSERT @s EXEC study.usp_Ingest_Entry N'41-1', N'T-E3', '2026-09-18T20:00:00', N'RAW three', 4;
INSERT @s EXEC study.usp_Ingest_Entry N'41-2', N'T-E4', '2026-09-18T21:00:00', N'RAW four', 1;
INSERT @s EXEC study.usp_Ingest_Entry N'42-1', N'T-E5', '2026-09-19T20:00:00', N'RAW five', 1;
REVERT;
DECLARE @ok BIT = CASE WHEN (SELECT COUNT(*) FROM @s WHERE s = 'INGESTED') = 5 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'00 setup: five entries ingested', @ok;
GO

/* ---------- Annotator types ---------- */
DECLARE @ok BIT = 0, @d NVARCHAR(400);
EXECUTE AS USER = 'coord_u';
BEGIN TRY
    DECLARE @x TABLE (id INT);
    INSERT @x EXEC study.usp_Register_Annotator N'106', 'TREATING_THERAPIST', 5, 0;
END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'01 a treating-therapist annotator type is refused', @ok;
GO

/* ---------- Before close ---------- */
DECLARE @q dbo.t_queue, @e dbo.t_entry;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 1;
INSERT @q EXEC study.usp_Get_Annotation_Queue 1;
INSERT @e EXEC study.usp_Get_Entry 1;
REVERT;
DECLARE @ok BIT = CASE WHEN NOT EXISTS (SELECT 1 FROM @q) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'02 before close, the rater queue is empty', @ok;
SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM @e) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'03 before close, a rater cannot open an entry', @ok;
GO

/* ---------- De-identification pipeline ---------- */
DECLARE @r dbo.t_deid, @ok BIT, @d NVARCHAR(400);
EXECUTE AS USER = 'deid_u';
INSERT @r EXEC study.usp_Get_Entries_For_Deid;
REVERT;
SET @ok = CASE WHEN (SELECT STRING_AGG(CAST(Study_Entry_ID AS VARCHAR(10)), ',') WITHIN GROUP (ORDER BY Study_Entry_ID) FROM @r) = '1,2,3,5'
               AND NOT EXISTS (SELECT 1 FROM @r WHERE Date_Offset_Days IS NULL) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'04 the pipeline gets B3 entries only (1,2,3,5), with date offsets', @ok;

EXECUTE AS USER = 'deid_u';
EXEC study.usp_Save_Deid_Text 1, N'DEID one: [PARTNER_1] called me at work', N'test-1';
EXEC study.usp_Save_Deid_Text 2, N'DEID two', N'test-1';
EXEC study.usp_Save_Deid_Text 3, N'DEID three', N'test-1';
EXEC study.usp_Save_Deid_Text 5, N'DEID five', N'test-1';
SET @ok = 0;
BEGIN TRY EXEC study.usp_Save_Deid_Text 4, N'DEID four', N'test-1'; END TRY
BEGIN CATCH SET @ok = CASE WHEN ERROR_NUMBER() = 51023 THEN 1 ELSE 0 END; SET @d = ERROR_MESSAGE(); END CATCH;
REVERT;
EXEC dbo.t_check N'05 de-identified text is refused without B3 permission', @ok, @d;

SET @ok = 0;
EXECUTE AS USER = 'deid_u';
BEGIN TRY DECLARE @n INT = (SELECT COUNT(*) FROM study.Study_Entry); END TRY
BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'06 the pipeline cannot read study tables directly', @ok;
GO

/* ---------- Close ---------- */
DECLARE @c TABLE (Closed_At DATETIME2(0));
DECLARE @s TABLE (s VARCHAR(30));
DECLARE @ok BIT = 0;
EXECUTE AS USER = 'coord_u';
EXEC sp_set_session_context N'actor_user_id', N'200';
INSERT @c EXEC study.usp_Close_Study;
BEGIN TRY INSERT @c EXEC study.usp_Close_Study; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'07 the study can be closed only once', @ok;

SET @ok = 0;
BEGIN TRY UPDATE study.Study_Close SET Closed_At = '2026-01-01'; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
BEGIN TRY DELETE FROM study.Study_Close; SET @ok = 0; END TRY BEGIN CATCH END CATCH;
EXEC dbo.t_check N'08 the close can''t be changed or undone, even by dbo', @ok;

SET @ok = CASE WHEN (SELECT Closed_By_User_ID FROM study.Study_Close) = 200 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'09 the close records who closed it', @ok;

EXECUTE AS USER = 'app_u';
INSERT @s EXEC study.usp_Ingest_Entry N'41-1', N'T-E6', '2026-10-01T20:00:00', N'RAW six', 1;
REVERT;
SET @ok = CASE WHEN (SELECT s FROM @s) = 'STUDY_CLOSED' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'10 ingestion stops after close', @ok;
GO

/* ---------- Rating ---------- */
DECLARE @q dbo.t_queue, @e dbo.t_entry, @e4 dbo.t_entry, @e3 dbo.t_entry, @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 1;
INSERT @q EXEC study.usp_Get_Annotation_Queue 1;
INSERT @e EXEC study.usp_Get_Entry 1;
INSERT @e4 EXEC study.usp_Get_Entry 4;
INSERT @e3 EXEC study.usp_Get_Entry 3;
REVERT;
SET @ok = CASE WHEN (SELECT STRING_AGG(CAST(Study_Entry_ID AS VARCHAR(10)), ',') WITHIN GROUP (ORDER BY Study_Entry_ID) FROM @q) = '1,2,5'
               THEN 1 ELSE 0 END;
EXEC dbo.t_check N'11 after close, rater 1 sees B3, de-identified, unrestricted entries (1,2,5)', @ok;
SET @ok = CASE WHEN (SELECT Deid_Text FROM @e) = N'DEID one: [PARTNER_1] called me at work' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'12 a rater gets the de-identified text, not the raw text', @ok;
SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM @e4) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'13 a rater cannot open an entry without B3 permission', @ok;
SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM @e3) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'14 a rater without restricted access cannot open a tier 4 entry', @ok;
GO

DECLARE @ok BIT;
SET @ok = CASE WHEN NOT EXISTS (
    SELECT 1
    FROM (VALUES (N'study.usp_Get_Annotation_Queue'), (N'study.usp_Get_Entry'), (N'study.usp_Get_Adjudication_Queue')) p(name)
    CROSS APPLY sys.dm_exec_describe_first_result_set_for_object(OBJECT_ID(p.name), 0) c
    WHERE c.name IN (N'Safety_Tier_At_Capture', N'Entry_Created_At', N'Entry_Text')) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'15 rating procedures return no app tier, entry date or raw text', @ok;
GO

DECLARE @v TABLE (v INT);
DECLARE @e dbo.t_entry, @ok BIT = 0;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 3;
INSERT @e EXEC study.usp_Get_Entry 3;
BEGIN TRY INSERT @v EXEC study.usp_Save_Annotation 1, 1, 0, 0, 0, 0, -3, 2, NULL; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'16 the adjudicator cannot save a rating', @ok;
SET @ok = CASE WHEN (SELECT Deid_Text FROM @e) = N'DEID three' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'17 the adjudicator with restricted access can open a tier 4 entry', @ok;
GO

DECLARE @v TABLE (v INT);
DECLARE @a dbo.t_ann, @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 1;
INSERT @v EXEC study.usp_Save_Annotation 1, 1, 0, 0, 0, 0, -3, 2, N'rater 1';
INSERT @v EXEC study.usp_Save_Annotation 2, 0, 0, 1, 1, 0,  4, 1, NULL;
REVERT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 2;
INSERT @v EXEC study.usp_Save_Annotation 1, 1, 0, 0, 0, 0, -5, 3, N'rater 2';
INSERT @v EXEC study.usp_Save_Annotation 2, 0, 0, 1, 1, 0,  4, 1, NULL;
INSERT @a EXEC study.usp_Get_Entry_Annotations 1;
REVERT;
SET @ok = CASE WHEN (SELECT COUNT(*) FROM @v) = 4 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'18 both raters save ratings', @ok;
SET @ok = CASE WHEN (SELECT COUNT(*) FROM @a) = 1 AND (SELECT Annotator_ID FROM @a) = 2 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'19 a rater sees only their own rating', @ok;
GO

DECLARE @q dbo.t_adj, @a dbo.t_ann, @ok BIT = 0;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 3;
INSERT @q EXEC study.usp_Get_Adjudication_Queue 0;
INSERT @a EXEC study.usp_Get_Entry_Annotations 1;
EXEC study.usp_Save_Gold_Label 1, 'ADJUDICATED', 1, 0, 0, 0, 0, -4, 3;
REVERT;
SET @ok = CASE WHEN (SELECT COUNT(*) FROM @q) = 2
               AND (SELECT Is_Unanimous FROM @q WHERE Study_Entry_ID = 1) = 0
               AND (SELECT Is_Unanimous FROM @q WHERE Study_Entry_ID = 2) = 1 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'20 the adjudication queue lists both double-rated entries', @ok;
SET @ok = CASE WHEN (SELECT COUNT(*) FROM @a) = 2 AND NOT EXISTS (SELECT 1 FROM @a WHERE Annotator_Type <> 'RATER') THEN 1 ELSE 0 END;
EXEC dbo.t_check N'21 the adjudicator sees both raters'' ratings', @ok;

SET @ok = 0;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 1;
BEGIN TRY EXEC study.usp_Save_Gold_Label 2, 'AGREEMENT', 0, 0, 1, 1, 0, 4, 1; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'22 a rater cannot save a gold label', @ok;
GO

DECLARE @q dbo.t_queue, @ok BIT, @d NVARCHAR(400);
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 4;
INSERT @q EXEC study.usp_Get_Annotation_Queue 1;
REVERT;
SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM @q) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'23 an unassigned rater sees nothing', @ok;

SET @ok = 0;
EXECUTE AS USER = 'deid_u';
BEGIN TRY EXEC study.usp_Save_Deid_Text 1, N'DEID one, revised', N'test-2'; END TRY
BEGIN CATCH SET @ok = CASE WHEN ERROR_NUMBER() = 51025 THEN 1 ELSE 0 END; SET @d = ERROR_MESSAGE(); END CATCH;
REVERT;
EXEC dbo.t_check N'24 de-identified text can''t be replaced once rated', @ok, @d;
GO

/* ---------- Coordinator views ---------- */
DECLARE @ok BIT;
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN (SELECT COUNT(*) FROM study.vw_Label_Pairs) = 2
               AND (SELECT R1_Safety_Tier FROM study.vw_Label_Pairs WHERE Study_Entry_ID = 1) = 2
               AND (SELECT R2_Safety_Tier FROM study.vw_Label_Pairs WHERE Study_Entry_ID = 1) = 3 THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'25 vw_Label_Pairs pairs the two raters', @ok;
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN EXISTS (SELECT 1 FROM study.vw_Annotation_Progress WHERE Study_Participant_ID = 1
                             AND Entries = 3 AND Rateable_Entries = 3 AND Labeled_Twice = 2 AND Gold_Final = 1)
               AND EXISTS (SELECT 1 FROM study.vw_Annotation_Progress WHERE Study_Participant_ID = 2 AND Rateable_Entries = 0)
          THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'26 vw_Annotation_Progress counts rateable, rated and gold entries', @ok;
GO

/* ---------- B3 revocation, withdrawal ---------- */
DECLARE @ok BIT = 0;
EXECUTE AS USER = 'app_u';
BEGIN TRY EXEC study.usp_Revoke_Text_Retention 1; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'27 the app cannot revoke B3 permission', @ok;

SET @ok = 0;
EXECUTE AS USER = 'coord_u';
BEGIN TRY EXEC study.usp_Record_Text_Retention 2, '2099-01-01', N'B3-v1'; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
EXEC study.usp_Revoke_Text_Retention 1;
REVERT;
EXEC dbo.t_check N'28 a future B3 signing time is refused', @ok;

DECLARE @q dbo.t_queue;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'annotator_id', 1;
INSERT @q EXEC study.usp_Get_Annotation_Queue 1;
REVERT;
SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM study.Entry_Deid d JOIN study.Study_Entry e ON e.Study_Entry_ID = d.Study_Entry_ID WHERE e.Study_Participant_ID = 1)
               AND NOT EXISTS (SELECT 1 FROM study.Annotation a JOIN study.Study_Entry e ON e.Study_Entry_ID = a.Study_Entry_ID WHERE e.Study_Participant_ID = 1)
               AND NOT EXISTS (SELECT 1 FROM study.Gold_Label g JOIN study.Study_Entry e ON e.Study_Entry_ID = g.Study_Entry_ID WHERE e.Study_Participant_ID = 1)
               AND (SELECT Text_Retention_At FROM study.Study_Participant WHERE Study_Participant_ID = 1) IS NULL
               AND (SELECT STRING_AGG(CAST(Study_Entry_ID AS VARCHAR(10)), ',') FROM @q) = '5'
          THEN 1 ELSE 0 END;
EXEC dbo.t_check N'29 revoking B3 deletes the de-identified text, ratings and gold labels', @ok;

SET @ok = 1;
EXECUTE AS USER = 'coord_u';
BEGIN TRY EXEC study.usp_Withdraw_Participant 3, 1; END TRY BEGIN CATCH SET @ok = 0; END CATCH;
REVERT;
SET @ok = CASE WHEN @ok = 1 AND NOT EXISTS (SELECT 1 FROM study.Entry_Deid WHERE Study_Entry_ID = 5)
                    AND NOT EXISTS (SELECT 1 FROM study.Study_Entry WHERE Study_Entry_ID = 5) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'30 withdrawal deletes de-identified text too', @ok;

SET @ok = CASE WHEN EXISTS (SELECT 1 FROM study.Access_Audit WHERE Action = 'CLOSE_STUDY' AND Actor_User_ID = 200)
               AND (SELECT COUNT(*) FROM study.Access_Audit WHERE Action = 'DEID_READ') = 4
               AND EXISTS (SELECT 1 FROM study.Access_Audit WHERE Action = 'REVOKE_TEXT_RETENTION') THEN 1 ELSE 0 END;
EXEC dbo.t_check N'31 the audit log records close, pipeline reads and B3 revocation', @ok;
GO
