/* =====================================================================
   Tests for study_patch_2026-10-28b_couple.sql (synthetic data only)
   Run as dbo on an EMPTY dev database:
     1. the schema, the 10-07, 10-17, 10-20 patches, test_main_stubs.sql,
        the 10-17b patch and the 10-28 patch;
     2. the SETUP block below (participants enrolled the old way);
     3. study_patch_2026-10-28b_couple.sql (twice, to check re-running);
     4. the TESTS block below.
   The two blocks are separated by a line reading only "-- @@" + "TESTS".
   ===================================================================== */
SET NOCOUNT ON;
GO
-- SETUP: client 41 enrolled partner by partner (both consented); client 42
-- with only partner 1 consented. Participants 1, 2 and 3.
CREATE USER app_u WITHOUT LOGIN;   ALTER ROLE study_app ADD MEMBER app_u;
CREATE USER coord_u WITHOUT LOGIN; ALTER ROLE study_coordinator ADD MEMBER coord_u;
GO
EXECUTE AS USER = 'coord_u';
DECLARE @ids TABLE (id INT);
INSERT @ids EXEC study.usp_Enroll_Participant N'41-1', N'v1', '2026-09-15T08:00:00', N'C-SSRS', '2026-09-14T07:00:00', 'BOTH_CONSENTED';
INSERT @ids EXEC study.usp_Enroll_Participant N'41-2', N'v1', '2026-09-15T09:00:00', N'C-SSRS', '2026-09-14T07:00:00', 'BOTH_CONSENTED';
INSERT @ids EXEC study.usp_Enroll_Participant N'42-1', N'v1', '2026-09-16T08:00:00', N'C-SSRS', '2026-09-12T07:00:00', 'SINGLE';
REVERT;
GO
-- @@TESTS
CREATE PROCEDURE dbo.t_check @name NVARCHAR(200), @ok BIT, @detail NVARCHAR(400) = NULL
AS PRINT CONCAT(CASE WHEN @ok = 1 THEN 'PASS ' ELSE 'FAIL ' END, @name,
                CASE WHEN @ok = 1 OR @detail IS NULL THEN '' ELSE ' :: ' + @detail END);
GO

DECLARE @got NVARCHAR(200) = (SELECT STRING_AGG(CONCAT(Study_Participant_ID, ':', Couple_ID, ':', Clinician_Attestation_Date), ',')
                                     WITHIN GROUP (ORDER BY Study_Participant_ID) FROM study.Study_Participant);
DECLARE @ok BIT = CASE WHEN @got = '1:1:2026-09-14,2:1:2026-09-14,3:3:2026-09-12' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'01 existing participants: partners of one client share a couple; screening date becomes the attestation date', @ok, @got;
GO

DECLARE @r TABLE (Couple_ID INT, P1 INT, P2 INT);
EXECUTE AS USER = 'coord_u';
INSERT @r EXEC study.usp_Enroll_Couple 50, N'v1', '2026-10-01T08:00:00', '2026-10-03T08:00:00', '2026-09-30';
REVERT;
DECLARE @ok BIT = CASE WHEN (SELECT COUNT(*) FROM @r WHERE Couple_ID = P1 AND P2 = P1 + 1) = 1
     AND (SELECT COUNT(DISTINCT sp.Couple_ID) FROM study.Study_Participant sp JOIN @r r ON sp.Study_Participant_ID IN (r.P1, r.P2)) = 1
     AND (SELECT COUNT(DISTINCT Date_Offset_Days) FROM study.Participant_Link WHERE Source_Client_ID IN (N'50-1', N'50-2')) = 1
     AND (SELECT COUNT(*) FROM study.Participant_Link WHERE Source_Client_ID IN (N'50-1', N'50-2')) = 2
     THEN 1 ELSE 0 END;
EXEC dbo.t_check N'02 a couple is enrolled in one step: two participants, one couple, one date offset', @ok;
GO

DECLARE @r TABLE (Couple_ID INT, P1 INT, P2 INT);
DECLARE @e1 INT = 0, @e2 INT = 0, @e3 INT = 0;
EXECUTE AS USER = 'coord_u';
BEGIN TRY INSERT @r EXEC study.usp_Enroll_Couple 50, N'v1', '2026-10-01T08:00:00', '2026-10-03T08:00:00', '2026-09-30'; END TRY BEGIN CATCH SET @e1 = ERROR_NUMBER(); END CATCH;
BEGIN TRY INSERT @r EXEC study.usp_Enroll_Couple 42, N'v1', '2026-10-01T08:00:00', '2026-10-03T08:00:00', '2026-09-30'; END TRY BEGIN CATCH SET @e3 = ERROR_NUMBER(); END CATCH;
DECLARE @tomorrow DATE = DATEADD(DAY, 2, CAST(SYSUTCDATETIME() AS DATE));
BEGIN TRY INSERT @r EXEC study.usp_Enroll_Couple 51, N'v1', '2026-10-01T08:00:00', NULL, '2026-09-30'; END TRY BEGIN CATCH SET @e2 = ERROR_NUMBER(); END CATCH;
REVERT;
DECLARE @e4 INT = 0;
EXECUTE AS USER = 'coord_u';
BEGIN TRY INSERT @r EXEC study.usp_Enroll_Couple 52, N'v1', '2026-10-01T08:00:00', '2026-10-03T08:00:00', @tomorrow; END TRY BEGIN CATCH SET @e4 = ERROR_NUMBER(); END CATCH;
REVERT;
DECLARE @got NVARCHAR(100) = CONCAT(@e1, ',', @e3, ',', @e2, ',', @e4);
DECLARE @ok BIT = CASE WHEN @got = '51032,51032,51030,51031' AND NOT EXISTS (SELECT 1 FROM @r) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'03 refused: a couple already (partly) enrolled, a missing partner consent, a future attestation', @ok, @got;
GO

EXECUTE AS USER = 'coord_u';
EXEC study.usp_Pause_Participant 5, 'SAFETY_ESCALATION';
REVERT;
DECLARE @both_paused BIT = CASE WHEN (SELECT COUNT(*) FROM study.Study_Participant WHERE Couple_ID = 4 AND Research_Paused_At IS NOT NULL AND Pause_Reason = 'SAFETY_ESCALATION') = 2 THEN 1 ELSE 0 END;
DECLARE @other_ok BIT = CASE WHEN NOT EXISTS (SELECT 1 FROM study.Study_Participant WHERE Couple_ID <> 4 AND Research_Paused_At IS NOT NULL) THEN 1 ELSE 0 END;
EXECUTE AS USER = 'coord_u';
EXEC study.usp_Resume_Participant 4;
REVERT;
DECLARE @ok BIT = CASE WHEN @both_paused = 1 AND @other_ok = 1
     AND NOT EXISTS (SELECT 1 FROM study.Study_Participant WHERE Research_Paused_At IS NOT NULL)
     AND (SELECT COUNT(*) FROM study.Access_Audit WHERE Action IN ('PAUSE','RESUME') AND Study_Participant_ID IN (4, 5)) = 4
     THEN 1 ELSE 0 END;
EXEC dbo.t_check N'04 pausing one partner pauses both; resuming the other resumes both; other couples untouched', @ok;
GO

-- B3 for partner 1 of couple 50 only.
EXECUTE AS USER = 'coord_u';
EXEC study.usp_Record_Text_Retention @Study_Participant_ID = 4, @Authorized_At = '2026-10-01T08:00:00', @Version = N'B3-v1';
REVERT;
DECLARE @s TABLE (st VARCHAR(30));
EXECUTE AS USER = 'app_u';
INSERT @s EXEC study.usp_Ingest_Entry N'50-1', N'e1', '2026-10-02T10:00:00', N'Before partner 2 consented.', 1;
INSERT @s EXEC study.usp_Ingest_Entry N'50-1', N'e2', '2026-10-04T10:00:00', N'Partner 1 words.', 1;
INSERT @s EXEC study.usp_Ingest_Entry N'50-2', N'e3', '2026-10-04T11:00:00', N'Partner 2 words.', 1;
REVERT;
DECLARE @got NVARCHAR(200) = (SELECT STRING_AGG(st, ',') FROM @s);
DECLARE @texts NVARCHAR(200);
EXECUTE AS USER = 'coord_u';
SET @texts = (SELECT STRING_AGG(CONCAT(Study_Participant_ID, ':', ISNULL(Entry_Text, N'-')), ',') WITHIN GROUP (ORDER BY Study_Entry_ID) FROM study.Study_Entry);
REVERT;
DECLARE @ok BIT = CASE WHEN @got = 'BEFORE_CONSENT,INGESTED,INGESTED_NO_TEXT' AND @texts = N'4:Partner 1 words.,5:-' THEN 1 ELSE 0 END;
SET @got = CONCAT(@got, ' | ', @texts);
EXEC dbo.t_check N'05 entries count from the later consent; each partner''s B3 choice is their own', @ok, @got;
GO

EXECUTE AS USER = 'coord_u';
EXEC study.usp_Withdraw_Participant @Study_Participant_ID = 5, @Delete_Linkage = 1;
REVERT;
DECLARE @entries INT;
EXECUTE AS USER = 'coord_u';
SET @entries = (SELECT COUNT(*) FROM study.Study_Entry WHERE Study_Participant_ID IN (4, 5));
REVERT;
DECLARE @ok BIT = CASE WHEN (SELECT COUNT(*) FROM study.Study_Participant WHERE Couple_ID = 4 AND Withdrawn_At IS NOT NULL) = 2
     AND @entries = 0
     AND NOT EXISTS (SELECT 1 FROM study.Participant_Link WHERE Study_Participant_ID IN (4, 5))
     AND NOT EXISTS (SELECT 1 FROM study.Study_Participant WHERE Couple_ID <> 4 AND Withdrawn_At IS NOT NULL)
     AND (SELECT COUNT(*) FROM study.Access_Audit WHERE Action = 'WITHDRAW' AND Study_Participant_ID IN (4, 5)) = 2
     THEN 1 ELSE 0 END;
DECLARE @got NVARCHAR(50) = CONCAT('entries=', @entries);
EXEC dbo.t_check N'06 one partner''s withdrawal ends both: both withdrawn, both partners'' entries and links deleted', @ok, @got;
GO

DECLARE @s TABLE (st VARCHAR(30));
EXECUTE AS USER = 'app_u';
INSERT @s EXEC study.usp_Ingest_Entry N'50-1', N'e4', '2026-10-05T10:00:00', N'After withdrawal.', 1;
REVERT;
EXECUTE AS USER = 'coord_u';
EXEC study.usp_Pause_Participant 4, 'OTHER';
REVERT;
DECLARE @ok BIT = CASE WHEN (SELECT st FROM @s) = 'NOT_ENROLLED'
     AND NOT EXISTS (SELECT 1 FROM study.Study_Participant WHERE Research_Paused_At IS NOT NULL) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'07 after withdrawal nothing is ingested and the couple can''t be paused', @ok;
GO
