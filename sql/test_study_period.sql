/* =====================================================================
   Tests for study_patch_2026-11-09_study_period.sql (synthetic data only)
   Run as dbo on an EMPTY dev database after the schema, every patch up
   to 2026-10-31g, test_main_stubs.sql and the 2026-11-09 patch.
   Every line should start with PASS. Search the output for FAIL.

   Couples: client 61 (therapist 7) enrolled 2026-10-01 and 10-03, then
            withdrawn with its links deleted;
            client 62 enrolled 2026-10-02, links deleted directly.
   ===================================================================== */
SET NOCOUNT ON;
GO
CREATE USER app_u WITHOUT LOGIN;   ALTER ROLE study_app ADD MEMBER app_u;
CREATE USER coord_u WITHOUT LOGIN; ALTER ROLE study_coordinator ADD MEMBER coord_u;
GO
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

/* ---------- Setup ---------- */
INSERT dbo.Therapist (Id, TherapistUserName) VALUES (7, N'theo@clinic.example');
INSERT dbo.Client (Id, TherapistID) VALUES (61, 7), (62, 7);
CREATE TABLE #couple (Couple_ID INT, P1 INT, P2 INT);
EXECUTE AS USER = 'coord_u';
INSERT #couple EXEC study.usp_Enroll_Couple 61, N'v1', '2026-10-01T08:00:00', '2026-10-03T08:00:00', '2026-09-30';
INSERT #couple EXEC study.usp_Enroll_Couple 62, N'v1', '2026-10-02T08:00:00', '2026-10-02T09:00:00', '2026-09-30';
REVERT;
INSERT dbo.SafetyAssessment (ClientId, IsClient1, IsClient2, RiskTier, RiskLabel, HasExtremeAbuse, Explanation, AssessedAt) VALUES
    (61, 1, 0, 5, N'extreme_abuse', 1, N'Threat described.',  '2026-10-05T21:00:00'),  -- study period
    (61, 1, 0, 4, N'crisis',        0, N'Before the study.',  '2026-09-20T21:00:00');  -- before consent

DECLARE @ok BIT = CASE WHEN (SELECT Study_Start_At FROM dbo.vw_Study_Couple WHERE ClientId = 61) = '2026-10-01T08:00:00'
                       THEN 1 ELSE 0 END;
EXEC dbo.t_check N'01 an enrolled couple starts at the earlier consent', @ok;

/* ---------- Withdraw with the links deleted ---------- */
DECLARE @p INT = (SELECT P1 FROM #couple WHERE Couple_ID = (SELECT MIN(Couple_ID) FROM #couple));
EXECUTE AS USER = 'coord_u';
EXEC study.usp_Withdraw_Participant @Study_Participant_ID = @p, @Delete_Linkage = 1;
REVERT;

SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM study.Participant_Link WHERE Source_Client_ID LIKE N'61-%')
                AND (SELECT Study_Start_At FROM dbo.vw_Study_Couple WHERE ClientId = 61) = '2026-10-01T08:00:00'
                AND (SELECT Study_Start_At FROM dbo.Study_Client_Period WHERE ClientId = 61) = '2026-10-01T08:00:00'
               THEN 1 ELSE 0 END;
EXEC dbo.t_check N'02 after withdrawal deletes the links, the couple keeps its study period', @ok;

-- The main API's filter (_SA_NOT_STUDY_PERIOD): only the pre-study flag may reach the dashboard.
DECLARE @shown NVARCHAR(200) = (SELECT STRING_AGG(Explanation, N'|') FROM dbo.SafetyAssessment
    WHERE ClientId = 61 AND NOT EXISTS (SELECT 1 FROM dbo.vw_Study_Couple sc
          WHERE sc.ClientId = SafetyAssessment.ClientId AND SafetyAssessment.AssessedAt >= sc.Study_Start_At));
SET @ok = CASE WHEN @shown = N'Before the study.' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'03 the study-period flag stays off the therapist dashboard', @ok, @shown;
GO

DECLARE @q dbo.t_esc, @ok BIT;
EXEC sp_set_session_context @key = N'actor_user_id', @value = N'1';
EXECUTE AS USER = 'app_u';
INSERT @q EXEC study.usp_Get_Escalation_Queue 1;
REVERT;
SET @ok = CASE WHEN EXISTS (SELECT 1 FROM @q WHERE ClientId = 61 AND Explanation = N'Threat described.' AND Study_Participant_ID IS NULL)
                AND NOT EXISTS (SELECT 1 FROM @q WHERE ClientId = 61 AND Explanation = N'Before the study.')
               THEN 1 ELSE 0 END;
EXEC dbo.t_check N'04 the study-period flag stays in the escalation queue, without a participant ID', @ok;
GO

/* ---------- Links deleted some other way ---------- */
DELETE FROM study.Participant_Link WHERE Source_Client_ID LIKE N'62-%';
DECLARE @ok BIT = CASE WHEN (SELECT Study_Start_At FROM dbo.vw_Study_Couple WHERE ClientId = 62) = '2026-10-02T08:00:00'
                       THEN 1 ELSE 0 END;
EXEC dbo.t_check N'05 any deletion of the links keeps the period', @ok;

-- A later start never replaces an earlier one.
UPDATE dbo.Study_Client_Period SET Study_Start_At = '2026-09-25T08:00:00' WHERE ClientId = 62;
SET @ok = CASE WHEN (SELECT Study_Start_At FROM dbo.vw_Study_Couple WHERE ClientId = 62) = '2026-09-25T08:00:00'
               THEN 1 ELSE 0 END;
EXEC dbo.t_check N'06 the view takes the earliest recorded start', @ok;
GO
