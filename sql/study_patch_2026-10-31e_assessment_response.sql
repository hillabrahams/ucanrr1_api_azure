/* =====================================================================
   UCANRR study schema: patch 2026-10-31e, research-scoped questionnaire
   responses (protocol §9.6; Attachment G, A6)

   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-31d_linkage_erasure.sql. Safe to re-run.

   dbo.Assessment_Response already stores every submitted questionnaire
   answer, but it's the client's real clinical record (the therapist
   reads it on the dashboard) -- exactly why study.Study_Entry exists
   instead of reusing the production journal table directly. This is
   the same move for questionnaire responses: a research-scoped copy,
   ingested the same way journal entries are (study.usp_Ingest_Entry /
   ucanrr1_api_with_roles_azure.py's /clients/{id}/study-entries), that
   can be deleted on withdrawal without touching the clinical record.

   Unlike Study_Entry there's no B3-style keep/discard choice --
   structured answers aren't free text, so there's nothing to
   generalize or de-identify the way journal text is. The row is kept
   whole or not ingested at all. No RLS: like Entry_Deid and Gold_Label,
   visibility is the coordinator's existing blanket schema SELECT, not
   a per-row filter predicate -- there's no annotator-rating workflow
   for questionnaire answers the way there is for journal entries.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. study.Study_Assessment_Response
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'study.Study_Assessment_Response', N'U') IS NULL
    CREATE TABLE study.Study_Assessment_Response (
        Study_Assessment_Response_ID BIGINT IDENTITY(1,1) CONSTRAINT PK_study_SAR PRIMARY KEY,
        Study_Participant_ID  INT           NOT NULL
            CONSTRAINT FK_study_SAR_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
        Source_Response_ID    NVARCHAR(128) NOT NULL CONSTRAINT UQ_study_SAR_Source UNIQUE,
        Assessment_Code       NVARCHAR(50)  NOT NULL,
        Assessment_Name       NVARCHAR(200) NOT NULL,
        Question_Text         NVARCHAR(500) NOT NULL,
        Question_Code         NVARCHAR(50)  NULL,
        Answer_Text           NVARCHAR(500) NOT NULL,
        Answer_Value          INT           NULL,
        Response_Created_At   DATETIME2(0)  NOT NULL,
        Ingested_At           DATETIME2(0)  NOT NULL CONSTRAINT DF_study_SAR_Ingested DEFAULT SYSUTCDATETIME()
    );
GO
IF OBJECT_ID(N'study.Study_Assessment_Response', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_study_SAR_Participant'
                   AND object_id = OBJECT_ID(N'study.Study_Assessment_Response'))
    CREATE INDEX IX_study_SAR_Participant
        ON study.Study_Assessment_Response (Study_Participant_ID, Response_Created_At);
GO

/* ---------------------------------------------------------------------
   2. Ingestion. Same shape and status vocabulary as usp_Ingest_Entry:
      NOT_ENROLLED / STUDY_CLOSED / WITHDRAWN / PAUSED / BEFORE_CONSENT /
      DUPLICATE / INGESTED.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Ingest_Assessment_Response
    @Source_Client_ID      NVARCHAR(128),
    @Source_Response_ID    NVARCHAR(128),
    @Assessment_Code       NVARCHAR(50),
    @Assessment_Name       NVARCHAR(200),
    @Question_Text         NVARCHAR(500),
    @Question_Code         NVARCHAR(50)  = NULL,
    @Answer_Text            NVARCHAR(500),
    @Answer_Value           INT = NULL,
    @Response_Created_At    DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @PID INT, @Consent DATETIME2(0), @Paused DATETIME2(0), @Withdrawn DATETIME2(0), @Status VARCHAR(30);

    SELECT @PID = sp.Study_Participant_ID,
           @Consent = (SELECT MAX(c.Research_Consent_At) FROM study.Study_Participant c WHERE c.Couple_ID = sp.Couple_ID),
           @Paused = sp.Research_Paused_At, @Withdrawn = sp.Withdrawn_At
    FROM study.Participant_Link pl
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = pl.Study_Participant_ID
    WHERE pl.Source_Client_ID = @Source_Client_ID;

    SET @Status = CASE
        WHEN @PID IS NULL                          THEN 'NOT_ENROLLED'
        WHEN EXISTS (SELECT 1 FROM study.Study_Close) THEN 'STUDY_CLOSED'
        WHEN @Withdrawn IS NOT NULL                THEN 'WITHDRAWN'
        WHEN @Paused IS NOT NULL                   THEN 'PAUSED'
        WHEN @Response_Created_At < @Consent       THEN 'BEFORE_CONSENT'
        ELSE NULL END;

    IF @Status IS NOT NULL
    BEGIN
        IF @Status <> 'NOT_ENROLLED'
            INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
            VALUES ('INGEST_SKIPPED', @PID, CONCAT(@Status, N'; ', @Assessment_Code));
        SELECT @Status AS Ingest_Status;
        RETURN;
    END;

    BEGIN TRY
        INSERT study.Study_Assessment_Response (Study_Participant_ID, Source_Response_ID, Assessment_Code,
            Assessment_Name, Question_Text, Question_Code, Answer_Text, Answer_Value, Response_Created_At)
        VALUES (@PID, @Source_Response_ID, @Assessment_Code, @Assessment_Name, @Question_Text,
            @Question_Code, @Answer_Text, @Answer_Value, @Response_Created_At);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        VALUES ('INGEST', @PID, CONCAT(N'assessment: ', @Assessment_Code));
        SELECT 'INGESTED' AS Ingest_Status;
    END TRY
    BEGIN CATCH
        -- Same idempotency approach as usp_Ingest_Entry: rely on the unique
        -- index rather than an existence check.
        IF ERROR_NUMBER() IN (2601, 2627)
            SELECT 'DUPLICATE' AS Ingest_Status;
        ELSE
            THROW;
    END CATCH;
END;
GO

GRANT EXECUTE ON study.usp_Ingest_Assessment_Response TO study_app;
GO

/* ---------------------------------------------------------------------
   3. Withdrawal erases the research copy too, same scope as every
      other table deleted there.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Withdraw_Participant
    @Study_Participant_ID INT,
    @Delete_Linkage BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Withdrawal must be run by a study coordinator.', 1;

    DECLARE @Couple TABLE (Study_Participant_ID INT PRIMARY KEY);
    INSERT @Couple
    SELECT sp.Study_Participant_ID FROM study.Study_Participant sp
    WHERE sp.Couple_ID = (SELECT Couple_ID FROM study.Study_Participant WHERE Study_Participant_ID = @Study_Participant_ID);

    BEGIN TRAN;
        DELETE g FROM study.Gold_Label g
          JOIN study.Study_Entry e ON e.Study_Entry_ID = g.Study_Entry_ID
         WHERE e.Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE a FROM study.Annotation a
          JOIN study.Study_Entry e ON e.Study_Entry_ID = a.Study_Entry_ID
         WHERE e.Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE d FROM study.Entry_Deid d
          JOIN study.Study_Entry e ON e.Study_Entry_ID = d.Study_Entry_ID
         WHERE e.Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE FROM study.Surrogate_Map WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE FROM study.Study_Assessment_Response WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE FROM study.Study_Entry WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        UPDATE study.Annotator_Assignment SET Revoked_At = SYSUTCDATETIME()
         WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple) AND Revoked_At IS NULL;

        UPDATE study.Study_Participant SET Withdrawn_At = SYSUTCDATETIME()
         WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple) AND Withdrawn_At IS NULL;

        IF @Delete_Linkage = 1
            DELETE FROM study.Participant_Link WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        SELECT 'WITHDRAW', c.Study_Participant_ID,
               CONCAT(CASE WHEN c.Study_Participant_ID = @Study_Participant_ID THEN N'requested'
                           ELSE CONCAT(N'with partner ', @Study_Participant_ID) END,
                      N'; ', CASE WHEN @Delete_Linkage = 1 THEN N'linkage deleted' ELSE N'linkage kept' END)
        FROM @Couple c;
    COMMIT;
END;
GO

/* Check: every row should say ok. */
SELECT 'Study_Assessment_Response' AS name,
       CASE WHEN OBJECT_ID('study.Study_Assessment_Response') IS NULL THEN 'MISSING' ELSE 'ok' END AS status
UNION ALL
SELECT 'usp_Ingest_Assessment_Response',
       CASE WHEN OBJECT_ID('study.usp_Ingest_Assessment_Response') IS NULL THEN 'MISSING' ELSE 'ok' END
UNION ALL
SELECT 'study_app has EXECUTE on usp_Ingest_Assessment_Response',
       CASE WHEN EXISTS (
            SELECT 1 FROM sys.database_permissions p
            JOIN sys.database_principals r ON r.principal_id = p.grantee_principal_id
            JOIN sys.objects o ON o.object_id = p.major_id
            WHERE r.name = N'study_app' AND p.permission_name = 'EXECUTE'
              AND o.name = N'usp_Ingest_Assessment_Response'
            ) THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'usp_Withdraw_Participant erases Study_Assessment_Response',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Withdraw_Participant')) LIKE N'%DELETE FROM study.Study_Assessment_Response%'
            THEN 'ok' ELSE 'MISSING' END;
GO
