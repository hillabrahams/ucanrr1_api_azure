/* =====================================================================
   UCANRR study schema: patch 2026-10-17b, safety flags to the
   escalation clinician (protocol §8.2, B4 §5, M6)
   Run as the server admin, in the study database, AFTER
   study_patch_2026-10-17_rating.sql. Safe to re-run.

   For study couples, safety flags go to the escalation clinician and
   never appear on the therapist dashboard:

     1. dbo.vw_Study_Couple: every couple with an enrolled partner (the
        link to the client account still exists), and when the couple's
        study participation started (the earliest research consent).
        The main API reads it to keep study-period assessments off the
        dashboard.
     2. Role "Study Escalation Clinician" in dbo.Roles (page
        escalation.html).
     3. study.Escalation_Review: when each tier 3 flag was first opened
        and reviewed, by whom, with an optional note.
     4. Procedures for the escalation page: the queue of tier 3 flags
        from the study period, and marking one reviewed. They return
        the flags and the model's explanation, never entry text.
     5. study.vw_Escalation_Record: the safety record for the monitor
        page (times only, no explanation text).

   Tier 1 and 2 flags of study couples are not routed anywhere (decision
   of 2026-10-17: tier 3 only).
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Study couples. Source_Client_ID is "<Client.Id>-<partner>".
      A couple stays a study couple while any partner's link exists,
      including after withdrawal without deleting the link.
   --------------------------------------------------------------------- */
CREATE OR ALTER VIEW dbo.vw_Study_Couple
AS
SELECT TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1)) AS ClientId,
       MIN(sp.Research_Consent_At) AS Study_Start_At
FROM study.Participant_Link pl
JOIN study.Study_Participant sp ON sp.Study_Participant_ID = pl.Study_Participant_ID
WHERE CHARINDEX('-', pl.Source_Client_ID) > 1
GROUP BY TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1));
GO

/* ---------------------------------------------------------------------
   2. The escalation clinician's role (sign-in routes to escalation.html)
   --------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM dbo.Roles WHERE RoleName = N'Study Escalation Clinician')
    INSERT dbo.Roles (RoleName, RedirectURL, Description, IsActive, CreatedAt)
    VALUES (N'Study Escalation Clinician', N'./escalation.html',
            N'Independent clinician who reviews tier 3 safety flags from study couples (protocol §8.2)',
            1, SYSUTCDATETIME());
GO

/* ---------------------------------------------------------------------
   3. Review record, one row per flag once it has been opened.
      No foreign key to dbo.SafetyAssessment, so the record survives if
      an assessment is deleted.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'study.Escalation_Review', N'U') IS NULL
    CREATE TABLE study.Escalation_Review (
        Safety_Assessment_ID     INT           NOT NULL CONSTRAINT PK_study_Escalation_Review PRIMARY KEY,
        First_Viewed_At          DATETIME2(0)  NOT NULL CONSTRAINT DF_study_ER_Viewed DEFAULT SYSUTCDATETIME(),
        First_Viewed_By_User_ID  INT           NULL,
        Reviewed_At              DATETIME2(0)  NULL,
        Reviewed_By_User_ID      INT           NULL,
        Review_Note              NVARCHAR(1000) NULL,
        CONSTRAINT CK_study_ER_Review CHECK ((Reviewed_At IS NULL AND Reviewed_By_User_ID IS NULL AND Review_Note IS NULL)
                                          OR (Reviewed_At IS NOT NULL AND Reviewed_By_User_ID IS NOT NULL))
    );
GO

-- Audit actions for the escalation page.
IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_study_Audit_Action')
    ALTER TABLE study.Access_Audit DROP CONSTRAINT CK_study_Audit_Action;
GO
ALTER TABLE study.Access_Audit ADD CONSTRAINT CK_study_Audit_Action CHECK (Action IN (
    'SESSION_START','SESSION_DENIED','VIEW_QUEUE','VIEW_ENTRY','VIEW_ENTRY_DENIED',
    'SAVE_ANNOTATION','SAVE_DENIED','SAVE_GOLD','INGEST','INGEST_SKIPPED',
    'ENROLL','ASSIGN','REVOKE_ASSIGNMENT','REGISTER_AGREEMENT','REVOKE_AGREEMENT',
    'REGISTER_ANNOTATOR','PAUSE','RESUME','WITHDRAW',
    'CLOSE_STUDY','RECORD_TEXT_RETENTION','REVOKE_TEXT_RETENTION','DEID_READ','DEID_SAVE',
    'VIEW_ESCALATIONS','REVIEW_ESCALATION'));
GO

/* ---------------------------------------------------------------------
   4. Procedures for escalation.html (study_app; the API allows only the
      Study Escalation Clinician role and sets 'actor_user_id').
   --------------------------------------------------------------------- */

/* Tier 3 flags of study couples from the study period, newest first,
   unreviewed before reviewed. Opening the queue records the first view
   of each flag it shows. */
CREATE OR ALTER PROCEDURE study.usp_Get_Escalation_Queue
    @Include_Reviewed BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Actor INT = TRY_CONVERT(INT, SESSION_CONTEXT(N'actor_user_id'));
    IF @Actor IS NULL THROW 51001, N'actor_user_id not set in session context.', 1;

    DECLARE @Q TABLE (Id INT PRIMARY KEY);
    INSERT @Q (Id)
    SELECT sa.Id
    FROM dbo.SafetyAssessment sa
    JOIN dbo.vw_Study_Couple c ON c.ClientId = sa.ClientId AND sa.AssessedAt >= c.Study_Start_At
    LEFT JOIN study.Escalation_Review r ON r.Safety_Assessment_ID = sa.Id
    WHERE sa.RiskTier >= 3
      AND (@Include_Reviewed = 1 OR r.Reviewed_At IS NULL);

    INSERT study.Escalation_Review (Safety_Assessment_ID, First_Viewed_By_User_ID)
    SELECT q.Id, @Actor FROM @Q q
    WHERE NOT EXISTS (SELECT 1 FROM study.Escalation_Review r WHERE r.Safety_Assessment_ID = q.Id);

    INSERT study.Access_Audit (Action, Detail) VALUES ('VIEW_ESCALATIONS', CONCAT(N'flags ', (SELECT COUNT(*) FROM @Q)));

    SELECT sa.Id AS Safety_Assessment_ID, sa.ClientId,
           CASE WHEN sa.IsClient1 = 1 THEN 1 WHEN sa.IsClient2 = 1 THEN 2 END AS Partner,
           spl.Study_Participant_ID,
           sa.RiskTier, sa.RiskLabel,
           sa.HasSuicidalIdeation, sa.HasSelfHarm, sa.HasOtherHarm, sa.HasExtremeAbuse,
           sa.HasHeatedArgument, sa.HasCrisisLanguage, sa.MentionsSubstanceUse,
           sa.MentionsWeaponAccess, sa.MentionsChildSafetyConcern, sa.AmbiguousLethalCuriosity,
           sa.Explanation, sa.AssessedAt,
           t.TherapistUserName,
           r.First_Viewed_At, r.Reviewed_At, r.Reviewed_By_User_ID, r.Review_Note
    FROM @Q q
    JOIN dbo.SafetyAssessment sa ON sa.Id = q.Id
    JOIN study.Escalation_Review r ON r.Safety_Assessment_ID = sa.Id
    LEFT JOIN dbo.Client cl ON cl.Id = sa.ClientId
    LEFT JOIN dbo.Therapist t ON t.Id = cl.TherapistID
    OUTER APPLY (SELECT TOP (1) pl.Study_Participant_ID FROM study.Participant_Link pl
                 WHERE pl.Source_Client_ID = CONCAT(sa.ClientId, '-',
                       CASE WHEN sa.IsClient1 = 1 THEN 1 WHEN sa.IsClient2 = 1 THEN 2 END)) spl
    ORDER BY CASE WHEN r.Reviewed_At IS NULL THEN 0 ELSE 1 END, sa.AssessedAt DESC, sa.Id DESC;
END;
GO

/* Mark one flag reviewed. Final: a review can't be changed. */
CREATE OR ALTER PROCEDURE study.usp_Mark_Escalation_Reviewed
    @Safety_Assessment_ID INT,
    @Note NVARCHAR(1000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Actor INT = TRY_CONVERT(INT, SESSION_CONTEXT(N'actor_user_id'));
    IF @Actor IS NULL THROW 51001, N'actor_user_id not set in session context.', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.SafetyAssessment sa
                   JOIN dbo.vw_Study_Couple c ON c.ClientId = sa.ClientId AND sa.AssessedAt >= c.Study_Start_At
                   WHERE sa.Id = @Safety_Assessment_ID AND sa.RiskTier >= 3)
        THROW 51002, N'Flag not found, or not a tier 3 flag from a study couple.', 1;

    SET @Note = NULLIF(LTRIM(RTRIM(@Note)), N'');

    BEGIN TRAN;
        IF EXISTS (SELECT 1 FROM study.Escalation_Review WITH (UPDLOCK, HOLDLOCK)
                   WHERE Safety_Assessment_ID = @Safety_Assessment_ID AND Reviewed_At IS NOT NULL)
            THROW 51028, N'This flag has already been reviewed.', 1;

        MERGE study.Escalation_Review AS t
        USING (SELECT @Safety_Assessment_ID AS Id) AS s ON t.Safety_Assessment_ID = s.Id
        WHEN MATCHED THEN UPDATE SET
            Reviewed_At = SYSUTCDATETIME(), Reviewed_By_User_ID = @Actor, Review_Note = @Note
        WHEN NOT MATCHED THEN INSERT
            (Safety_Assessment_ID, First_Viewed_By_User_ID, Reviewed_At, Reviewed_By_User_ID, Review_Note)
            VALUES (@Safety_Assessment_ID, @Actor, SYSUTCDATETIME(), @Actor, @Note);

        INSERT study.Access_Audit (Action, Detail)
        VALUES ('REVIEW_ESCALATION', CONCAT(N'safety assessment ', @Safety_Assessment_ID));
    COMMIT;

    SELECT Reviewed_At FROM study.Escalation_Review WHERE Safety_Assessment_ID = @Safety_Assessment_ID;
END;
GO

/* ---------------------------------------------------------------------
   5. Safety record for the monitor page (coordinator): times only.
   --------------------------------------------------------------------- */
CREATE OR ALTER VIEW study.vw_Escalation_Record
AS
SELECT sa.Id AS Safety_Assessment_ID, sa.ClientId, sa.RiskTier, sa.AssessedAt,
       r.First_Viewed_At, r.Reviewed_At,
       DATEDIFF(MINUTE, sa.AssessedAt, r.Reviewed_At) AS Minutes_To_Review
FROM dbo.SafetyAssessment sa
JOIN dbo.vw_Study_Couple c ON c.ClientId = sa.ClientId AND sa.AssessedAt >= c.Study_Start_At
LEFT JOIN study.Escalation_Review r ON r.Safety_Assessment_ID = sa.Id
WHERE sa.RiskTier >= 3;
GO

/* ---------------------------------------------------------------------
   6. Permissions
   --------------------------------------------------------------------- */
GRANT EXECUTE ON study.usp_Get_Escalation_Queue      TO study_app;
GRANT EXECUTE ON study.usp_Mark_Escalation_Reviewed  TO study_app;
-- The coordinator's schema-level SELECT covers study.vw_Escalation_Record.
-- The main API's login reads dbo.vw_Study_Couple like the other dbo objects.
GO

/* Check: every row should say ok. */
SELECT v.name,
       CASE WHEN OBJECT_ID(v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('dbo.vw_Study_Couple'), ('study.Escalation_Review'), ('study.usp_Get_Escalation_Queue'),
             ('study.usp_Mark_Escalation_Reviewed'), ('study.vw_Escalation_Record')) v(name)
UNION ALL
SELECT 'Roles: Study Escalation Clinician',
       CASE WHEN EXISTS (SELECT 1 FROM dbo.Roles WHERE RoleName = N'Study Escalation Clinician') THEN 'ok' ELSE 'MISSING' END;
GO
