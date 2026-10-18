/* =====================================================================
   Tests for study_patch_2026-10-17b_escalation.sql (synthetic data only)
   Run as dbo on an EMPTY dev database after the schema, the 10-07 and
   10-17 patches, test_main_stubs.sql and the 10-17b patch.
   Every line should start with PASS. Search the output for FAIL.

   Couples: client 41 (therapist 7) enrolled 2026-09-15, partner 1;
            client 42 enrolled 2026-09-20, partner 2;
            client 43 not in the study.
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
INSERT dbo.Therapist (Id, TherapistUserName) VALUES (7, N'theo@clinic.example'), (8, N'other@clinic.example');
INSERT dbo.Client (Id, TherapistID) VALUES (41, 7), (42, 8), (43, 7);
EXECUTE AS USER = 'coord_u';
DECLARE @ids TABLE (id INT);
INSERT @ids EXEC study.usp_Enroll_Participant N'41-1', N'v1', '2026-09-15T08:00:00', N'TEST', '2026-09-15T07:00:00', 'BOTH_CONSENTED';
INSERT @ids EXEC study.usp_Enroll_Participant N'42-2', N'v1', '2026-09-20T08:00:00', N'TEST', '2026-09-20T07:00:00', 'BOTH_CONSENTED';
REVERT;
-- Ids 1..6
INSERT dbo.SafetyAssessment (ClientId, IsClient1, IsClient2, RiskTier, RiskLabel, HasSelfHarm, Explanation, AssessedAt) VALUES
    (41, 1, 0, 3, N'crisis',         1, N'Mentions wanting to hurt self.', '2026-09-16T21:00:00'),  -- 1 study, tier 3
    (41, 1, 0, 3, N'crisis',         1, N'Before the study.',              '2026-09-10T21:00:00'),  -- 2 before consent
    (41, 0, 1, 2, N'heated',         0, N'Loud argument.',                 '2026-09-17T21:00:00'),  -- 3 study, tier 2
    (43, 1, 0, 3, N'extreme_abuse',  0, N'Not a study couple.',            '2026-09-18T21:00:00'),  -- 4 not in study
    (42, 0, 1, 3, N'extreme_abuse',  0, N'Threat described.',              '2026-09-21T21:00:00'),  -- 5 study, tier 3
    (42, 0, 1, 3, N'crisis',         0, N'Before consent.',                '2026-09-19T21:00:00');  -- 6 before consent
GO

/* ---------- Study couples ---------- */
DECLARE @ok BIT = CASE WHEN (SELECT COUNT(*) FROM dbo.vw_Study_Couple) = 2
    AND (SELECT Study_Start_At FROM dbo.vw_Study_Couple WHERE ClientId = 41) = '2026-09-15T08:00:00'
    AND (SELECT Study_Start_At FROM dbo.vw_Study_Couple WHERE ClientId = 42) = '2026-09-20T08:00:00' THEN 1 ELSE 0 END;
EXEC dbo.t_check N'01 vw_Study_Couple lists enrolled couples with their start', @ok;
SET @ok = CASE WHEN EXISTS (SELECT 1 FROM dbo.Roles WHERE RoleName = N'Study Escalation Clinician'
                            AND RedirectURL = N'./escalation.html' AND IsActive = 1) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'02 the Study Escalation Clinician role exists', @ok;
GO

/* ---------- Queue ---------- */
DECLARE @q dbo.t_esc, @ok BIT = 0;
EXECUTE AS USER = 'app_u';
BEGIN TRY INSERT @q EXEC study.usp_Get_Escalation_Queue 0; END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'03 the queue needs a signed-in actor', @ok;
GO
DECLARE @q dbo.t_esc, @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
INSERT @q EXEC study.usp_Get_Escalation_Queue 0;
REVERT;
SET @ok = CASE WHEN (SELECT STRING_AGG(CAST(Safety_Assessment_ID AS VARCHAR(10)), ',') WITHIN GROUP (ORDER BY Safety_Assessment_ID) FROM @q) = '1,5'
               THEN 1 ELSE 0 END;
EXEC dbo.t_check N'04 the queue holds only tier 3 flags of study couples from the study period (1,5)', @ok;
SET @ok = CASE WHEN EXISTS (SELECT 1 FROM @q WHERE Safety_Assessment_ID = 1 AND Partner = 1 AND Study_Participant_ID = 1
                            AND TherapistUserName = N'theo@clinic.example' AND HasSelfHarm = 1 AND First_Viewed_At IS NOT NULL)
               AND EXISTS (SELECT 1 FROM @q WHERE Safety_Assessment_ID = 5 AND Partner = 2 AND Study_Participant_ID = 2)
          THEN 1 ELSE 0 END;
EXEC dbo.t_check N'05 queue rows carry partner, participant, therapist, flags and first view', @ok;
SET @ok = CASE WHEN (SELECT COUNT(*) FROM study.Escalation_Review WHERE First_Viewed_By_User_ID = 300) = 2 THEN 1 ELSE 0 END;
EXEC dbo.t_check N'06 opening the queue records the first view', @ok;
GO

/* ---------- Review ---------- */
DECLARE @r TABLE (Reviewed_At DATETIME2(0));
DECLARE @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
INSERT @r EXEC study.usp_Mark_Escalation_Reviewed 1, N'  Called the therapist.  ';
REVERT;
SET @ok = CASE WHEN EXISTS (SELECT 1 FROM study.Escalation_Review WHERE Safety_Assessment_ID = 1 AND Reviewed_By_User_ID = 300
                            AND Review_Note = N'Called the therapist.' AND Reviewed_At IS NOT NULL) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'07 a flag is marked reviewed with a trimmed note', @ok;
GO
DECLARE @r TABLE (Reviewed_At DATETIME2(0));
DECLARE @ok BIT = 0, @d NVARCHAR(400);
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
BEGIN TRY INSERT @r EXEC study.usp_Mark_Escalation_Reviewed 1, NULL; END TRY
BEGIN CATCH SET @ok = CASE WHEN ERROR_NUMBER() = 51028 THEN 1 ELSE 0 END; SET @d = ERROR_MESSAGE(); END CATCH;
REVERT;
EXEC dbo.t_check N'08 a review can''t be changed', @ok, @d;
SET @ok = 0;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
BEGIN TRY INSERT @r EXEC study.usp_Mark_Escalation_Reviewed 4, NULL; END TRY
BEGIN CATCH SET @ok = CASE WHEN ERROR_NUMBER() = 51002 THEN 1 ELSE 0 END; END CATCH;
REVERT;
EXEC dbo.t_check N'09 a non-study flag can''t be reviewed here', @ok;
SET @ok = 0;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
BEGIN TRY INSERT @r EXEC study.usp_Mark_Escalation_Reviewed 3, NULL; END TRY
BEGIN CATCH SET @ok = CASE WHEN ERROR_NUMBER() = 51002 THEN 1 ELSE 0 END; END CATCH;
REVERT;
EXEC dbo.t_check N'10 a tier 2 flag can''t be reviewed here', @ok;
GO
DECLARE @q dbo.t_esc, @all dbo.t_esc, @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
INSERT @q EXEC study.usp_Get_Escalation_Queue 0;
INSERT @all EXEC study.usp_Get_Escalation_Queue 1;
REVERT;
SET @ok = CASE WHEN (SELECT STRING_AGG(CAST(Safety_Assessment_ID AS VARCHAR(10)), ',') FROM @q) = '5'
               AND (SELECT COUNT(*) FROM @all) = 2
               AND (SELECT TOP (1) Safety_Assessment_ID FROM @all ORDER BY CASE WHEN Reviewed_At IS NULL THEN 0 ELSE 1 END) = 5
          THEN 1 ELSE 0 END;
EXEC dbo.t_check N'11 reviewed flags leave the default queue but stay in the full one', @ok;
GO

/* ---------- Safety record, withdrawal ---------- */
DECLARE @ok BIT;
EXECUTE AS USER = 'coord_u';
SET @ok = CASE WHEN (SELECT COUNT(*) FROM study.vw_Escalation_Record) = 2
               AND (SELECT Minutes_To_Review FROM study.vw_Escalation_Record WHERE Safety_Assessment_ID = 1) IS NOT NULL
               AND (SELECT Minutes_To_Review FROM study.vw_Escalation_Record WHERE Safety_Assessment_ID = 5) IS NULL THEN 1 ELSE 0 END;
REVERT;
EXEC dbo.t_check N'12 the coordinator reads the safety record', @ok;
SET @ok = 0;
EXECUTE AS USER = 'app_u';
BEGIN TRY DECLARE @n INT = (SELECT COUNT(*) FROM study.Escalation_Review); END TRY BEGIN CATCH SET @ok = 1; END CATCH;
REVERT;
EXEC dbo.t_check N'13 the app can''t read the review table directly', @ok;
GO
EXECUTE AS USER = 'coord_u';
EXEC study.usp_Withdraw_Participant 2, 1;
REVERT;
DECLARE @q dbo.t_esc, @ok BIT;
EXECUTE AS USER = 'app_u';
EXEC sp_set_session_context N'actor_user_id', N'300';
INSERT @q EXEC study.usp_Get_Escalation_Queue 1;
REVERT;
SET @ok = CASE WHEN NOT EXISTS (SELECT 1 FROM dbo.vw_Study_Couple WHERE ClientId = 42)
               AND NOT EXISTS (SELECT 1 FROM @q WHERE ClientId = 42) THEN 1 ELSE 0 END;
EXEC dbo.t_check N'14 withdrawal with the link deleted ends the couple''s study routing', @ok;
GO
