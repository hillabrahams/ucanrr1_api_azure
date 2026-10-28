/* =====================================================================
   Tests for study_patch_2026-10-28_five_tier.sql (synthetic data only)
   Run as dbo on an EMPTY dev database:
     1. the schema, the 10-07, 10-17, 10-20 patches, test_main_stubs.sql
        and the 10-17b patch;
     2. the SETUP block below (old-scale rows, as the live data is today);
     3. study_patch_2026-10-28_five_tier.sql (twice, to check re-running);
     4. the TESTS block below.
   The two blocks are separated by a line reading only "-- @@" + "TESTS".
   ===================================================================== */
SET NOCOUNT ON;
GO
-- SETUP: couple 41 enrolled 2026-09-15; rows on the old 0–3 scale.
CREATE USER app_u WITHOUT LOGIN;   ALTER ROLE study_app ADD MEMBER app_u;
CREATE USER coord_u WITHOUT LOGIN; ALTER ROLE study_coordinator ADD MEMBER coord_u;
GO
INSERT dbo.Therapist (Id, TherapistUserName) VALUES (7, N'theo@clinic.example');
INSERT dbo.Client (Id, TherapistID) VALUES (41, 7);
EXECUTE AS USER = 'coord_u';
DECLARE @ids TABLE (id INT);
INSERT @ids EXEC study.usp_Enroll_Participant N'41-1', N'v1', '2026-09-15T08:00:00', N'T', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
REVERT;
INSERT dbo.SafetyAssessment (ClientId, IsClient1, RiskTier, RiskLabel, AssessedAt) VALUES
    (41, 1, 0, N'normal',            '2026-09-16T10:00:00'),  -- 1 -> 1
    (41, 1, 1, N'ambiguous_monitor', '2026-09-16T11:00:00'),  -- 2 -> 3
    (41, 1, 2, N'heated',            '2026-09-16T12:00:00'),  -- 3 -> 2
    (41, 1, 3, N'crisis',            '2026-09-16T13:00:00'),  -- 4 -> 4
    (41, 1, 3, N'extreme_abuse',     '2026-09-16T14:00:00'),  -- 5 -> 5
    (41, 1, 3, N'phq9_item9',        '2026-09-16T15:00:00'),  -- 6 -> 4
    (41, 1, 2, N'Something Else',    '2026-09-16T16:00:00');  -- 7 unchanged
GO
-- @@TESTS
CREATE PROCEDURE dbo.t_check @name NVARCHAR(200), @ok BIT, @detail NVARCHAR(400) = NULL
AS PRINT CONCAT(CASE WHEN @ok = 1 THEN 'PASS ' ELSE 'FAIL ' END, @name,
                CASE WHEN @ok = 1 OR @detail IS NULL THEN '' ELSE ' :: ' + @detail END);
GO
CREATE TYPE dbo.t_esc AS TABLE (
    Safety_Assessment_ID INT, ClientId INT, Partner INT, Study_Participant_ID INT, RiskTier INT, RiskLabel NVARCHAR(100),
    HasSuicidalIdeation BIT, HasSelfHarm BIT, HasOtherHarm BIT, HasExtremeAbuse BIT, HasHeatedArgument BIT,
    HasCrisisLanguage BIT, MentionsSubstanceUse BIT, MentionsWeaponAccess BIT, MentionsChildSafetyConcern BIT,
    AmbiguousLethalCuriosity BIT, Explanation NVARCHAR(MAX), AssessedAt DATETIME2, TherapistUserName NVARCHAR(200),
    First_Viewed_At DATETIME2(0), Reviewed_At DATETIME2(0), Reviewed_By_User_ID INT, Review_Note NVARCHAR(1000));
GO

DECLARE @got NVARCHAR(100) = (SELECT STRING_AGG(CAST(RiskTier AS VARCHAR(2)), ',') WITHIN GROUP (ORDER BY Id) FROM dbo.SafetyAssessment);
DECLARE @ok BIT = CASE WHEN @got = '1,3,2,4,5,4,2' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'01 existing rows are converted by label; an unknown label is left alone', @ok, @got;
GO

-- An app build from before 2026-10-28 stores a crisis as 3 before the API conversion is live.
INSERT dbo.SafetyAssessment (ClientId, IsClient1, RiskTier, RiskLabel, AssessedAt)
VALUES (41, 1, 3, N'crisis', '2026-09-17T10:00:00');   -- 8
GO
DECLARE @q dbo.t_esc, @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
INSERT @q EXEC study.usp_Get_Escalation_Queue 0;
REVERT;
DECLARE @got NVARCHAR(100) = (SELECT STRING_AGG(CAST(Safety_Assessment_ID AS VARCHAR(3)), ',') WITHIN GROUP (ORDER BY Safety_Assessment_ID) FROM @q);
SET @ok = CASE WHEN @got = '4,5,6,8' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'02 the queue holds Crisis, Extreme Abuse and item 9 (and an old-scale crisis), not Ambiguous Monitor', @ok, @got;
GO

DECLARE @r TABLE (Reviewed_At DATETIME2(0));
DECLARE @ok BIT = 1;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
BEGIN TRY INSERT @r EXEC study.usp_Mark_Escalation_Reviewed 5, NULL; END TRY BEGIN CATCH SET @ok = 0; END CATCH;
BEGIN TRY INSERT @r EXEC study.usp_Mark_Escalation_Reviewed 2, NULL; SET @ok = 0; END TRY BEGIN CATCH END CATCH;
REVERT;
EXEC dbo.t_check N'03 an Extreme Abuse flag can be reviewed; an Ambiguous Monitor (tier 3) one can''t', @ok;
GO

DECLARE @ok BIT;
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN (SELECT COUNT(*) FROM study.vw_Escalation_Record) = 4
               AND (SELECT Minutes_To_Review FROM study.vw_Escalation_Record WHERE Safety_Assessment_ID = 5) IS NOT NULL
          THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'04 the safety record counts the four crisis-level flags', @ok;
GO
