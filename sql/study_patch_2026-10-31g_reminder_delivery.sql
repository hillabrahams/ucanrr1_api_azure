/* =====================================================================
   UCANRR study schema: patch 2026-10-31g, reminder delivery records
   (Attachment G, B2)

   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-31f_interview_transcript.sql. Safe to re-run.

   The mobile app only ever scheduled local, on-device reminders --
   nothing recorded whether a reminder actually fired or was seen.
   flutter_local_notifications gives no native "this was delivered"
   callback for a scheduled notification, so the app infers it: on
   each launch it compares the time it last expected a reminder to
   fire against now, and logs an inferred-fired record for anything
   that should have already happened, plus a separate record whenever
   the participant actually taps a reminder. Both kinds sync here,
   the same way journal entries and questionnaire responses do: a
   research-scoped copy, ingested through a dedicated endpoint, that
   can be deleted on withdrawal.

   No per-row production table backs this the way Event or
   Assessment_Response do for the other two ingestion paths -- there's
   nothing server-side to join against for ownership, since reminders
   only ever existed as local device state. The client-supplied
   Source_Event_ID is just a dedup key (so a retried sync can't create
   duplicates); the usual enrollment/consent/withdrawal gating still
   happens in usp_Ingest_Reminder_Delivery like every other ingestion
   proc.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. study.Study_Reminder_Delivery
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'study.Study_Reminder_Delivery', N'U') IS NULL
    CREATE TABLE study.Study_Reminder_Delivery (
        Study_Reminder_Delivery_ID BIGINT IDENTITY(1,1) CONSTRAINT PK_study_SRD PRIMARY KEY,
        Study_Participant_ID  INT           NOT NULL
            CONSTRAINT FK_study_SRD_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
        Source_Event_ID       NVARCHAR(128) NOT NULL CONSTRAINT UQ_study_SRD_Source UNIQUE,
        Reminder_Type         VARCHAR(10)   NOT NULL
            CONSTRAINT CK_study_SRD_Reminder_Type CHECK (Reminder_Type IN ('DAILY', 'WEEKLY')),
        Event_Type             VARCHAR(20)   NOT NULL
            CONSTRAINT CK_study_SRD_Event_Type CHECK (Event_Type IN ('FIRED_INFERRED', 'TAPPED')),
        Scheduled_At           DATETIME2(0)  NOT NULL,
        Detected_At            DATETIME2(0)  NOT NULL,
        Ingested_At            DATETIME2(0)  NOT NULL CONSTRAINT DF_study_SRD_Ingested DEFAULT SYSUTCDATETIME()
    );
GO
IF OBJECT_ID(N'study.Study_Reminder_Delivery', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_study_SRD_Participant'
                   AND object_id = OBJECT_ID(N'study.Study_Reminder_Delivery'))
    CREATE INDEX IX_study_SRD_Participant
        ON study.Study_Reminder_Delivery (Study_Participant_ID, Scheduled_At);
GO

/* ---------------------------------------------------------------------
   2. Ingestion. Same shape and status vocabulary as usp_Ingest_Entry /
      usp_Ingest_Assessment_Response: NOT_ENROLLED / STUDY_CLOSED /
      WITHDRAWN / PAUSED / BEFORE_CONSENT / DUPLICATE / INGESTED.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Ingest_Reminder_Delivery
    @Source_Client_ID   NVARCHAR(128),
    @Source_Event_ID     NVARCHAR(128),
    @Reminder_Type       VARCHAR(10),
    @Event_Type           VARCHAR(20),
    @Scheduled_At         DATETIME2(0),
    @Detected_At          DATETIME2(0)
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
        WHEN @Scheduled_At < @Consent               THEN 'BEFORE_CONSENT'
        ELSE NULL END;

    IF @Status IS NOT NULL
    BEGIN
        IF @Status <> 'NOT_ENROLLED'
            INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
            VALUES ('INGEST_SKIPPED', @PID, CONCAT(@Status, N'; ', @Reminder_Type, N'/', @Event_Type));
        SELECT @Status AS Ingest_Status;
        RETURN;
    END;

    BEGIN TRY
        INSERT study.Study_Reminder_Delivery (Study_Participant_ID, Source_Event_ID, Reminder_Type,
            Event_Type, Scheduled_At, Detected_At)
        VALUES (@PID, @Source_Event_ID, @Reminder_Type, @Event_Type, @Scheduled_At, @Detected_At);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        VALUES ('INGEST', @PID, CONCAT(N'reminder: ', @Reminder_Type, N'/', @Event_Type));
        SELECT 'INGESTED' AS Ingest_Status;
    END TRY
    BEGIN CATCH
        -- Same idempotency approach as the other ingestion procs: rely on
        -- the unique index rather than an existence check, since a retried
        -- sync will resend the same Source_Event_ID.
        IF ERROR_NUMBER() IN (2601, 2627)
            SELECT 'DUPLICATE' AS Ingest_Status;
        ELSE
            THROW;
    END CATCH;
END;
GO

GRANT EXECUTE ON study.usp_Ingest_Reminder_Delivery TO study_app;
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

        DELETE FROM study.Study_Reminder_Delivery WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE td FROM study.Interview_Transcript_Deid td
          JOIN study.Interview_Transcript t ON t.Interview_Transcript_ID = td.Interview_Transcript_ID
         WHERE t.Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

        DELETE FROM study.Interview_Transcript WHERE Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

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
SELECT 'Study_Reminder_Delivery' AS name,
       CASE WHEN OBJECT_ID('study.Study_Reminder_Delivery') IS NULL THEN 'MISSING' ELSE 'ok' END AS status
UNION ALL
SELECT 'usp_Ingest_Reminder_Delivery',
       CASE WHEN OBJECT_ID('study.usp_Ingest_Reminder_Delivery') IS NULL THEN 'MISSING' ELSE 'ok' END
UNION ALL
SELECT 'study_app has EXECUTE on usp_Ingest_Reminder_Delivery',
       CASE WHEN EXISTS (
            SELECT 1 FROM sys.database_permissions p
            JOIN sys.database_principals r ON r.principal_id = p.grantee_principal_id
            JOIN sys.objects o ON o.object_id = p.major_id
            WHERE r.name = N'study_app' AND p.permission_name = 'EXECUTE'
              AND o.name = N'usp_Ingest_Reminder_Delivery'
            ) THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'usp_Withdraw_Participant erases Study_Reminder_Delivery',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Withdraw_Participant')) LIKE N'%DELETE FROM study.Study_Reminder_Delivery%'
            THEN 'ok' ELSE 'MISSING' END;
GO
