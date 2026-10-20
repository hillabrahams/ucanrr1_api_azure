/* =====================================================================
   UCANRR study schema: patch 2026-10-20, entry text only with B3
   permission (conflict row 6; B2 §5, B3)
   Run as the server admin, in the study database, AFTER
   study_patch_2026-10-17b_escalation.sql. Safe to re-run.

   B2 §5 and B3: by default the words of an entry are not kept; they are
   kept only with the partner's B3 permission. Until now every ingested
   entry stored its full text.

     1. study.Study_Entry.Entry_Text may be NULL.
     2. usp_Ingest_Entry keeps the text only when the participant has B3
        permission and the entry was written at or after the B3 signing
        time. Otherwise the entry is still counted (date, capture tier,
        restriction), without its words.
     3. usp_Revoke_Text_Retention also erases the kept text of the
        participant's entries, not only the de-identified copies.
     4. The de-identification procedures only handle entries whose text
        was kept.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Entry_Text optional. The security policy is schema-bound to
      Study_Entry, so drop it first; it is recreated at the end.
   --------------------------------------------------------------------- */
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'pol_study_access'
           AND schema_id = SCHEMA_ID(N'study_sec'))
    DROP SECURITY POLICY study_sec.pol_study_access;
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'study.Study_Entry')
           AND name = N'Entry_Text' AND is_nullable = 0)
    ALTER TABLE study.Study_Entry ALTER COLUMN Entry_Text NVARCHAR(MAX) NULL;
    /* PRODUCTION with Always Encrypted: the column keeps its encryption
       setting; add NULL to its existing ENCRYPTED WITH definition. */
GO

/* ---------------------------------------------------------------------
   2. Ingestion: text only with B3 permission.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Ingest_Entry
    @Source_Client_ID        NVARCHAR(128),
    @Source_Entry_ID         NVARCHAR(128),
    @Entry_Created_At        DATETIME2(0),
    @Entry_Text              NVARCHAR(MAX),
    @Safety_Tier_At_Capture  TINYINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @PID INT, @Consent DATETIME2(0), @Paused DATETIME2(0), @Withdrawn DATETIME2(0),
            @Retention DATETIME2(0), @Status VARCHAR(30);

    SELECT @PID = sp.Study_Participant_ID, @Consent = sp.Research_Consent_At,
           @Paused = sp.Research_Paused_At, @Withdrawn = sp.Withdrawn_At,
           @Retention = sp.Text_Retention_At
    FROM study.Participant_Link pl
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = pl.Study_Participant_ID
    WHERE pl.Source_Client_ID = @Source_Client_ID;

    SET @Status = CASE
        WHEN @PID IS NULL                          THEN 'NOT_ENROLLED'
        WHEN EXISTS (SELECT 1 FROM study.Study_Close) THEN 'STUDY_CLOSED'
        WHEN @Withdrawn IS NOT NULL                THEN 'WITHDRAWN'
        WHEN @Paused IS NOT NULL                   THEN 'PAUSED'
        WHEN @Entry_Created_At < @Consent          THEN 'BEFORE_CONSENT'
        ELSE NULL END;

    IF @Status IS NOT NULL
    BEGIN
        -- NOT_ENROLLED is the normal case for most users: don't log it.
        IF @Status <> 'NOT_ENROLLED'
            INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
            VALUES ('INGEST_SKIPPED', @PID, @Status);
        SELECT @Status AS Ingest_Status;
        RETURN;
    END;

    -- B2 §5, B3: the words are kept only with permission, from its signing on.
    DECLARE @Keep_Text BIT = CASE WHEN @Retention IS NOT NULL AND @Entry_Created_At >= @Retention
                                  THEN 1 ELSE 0 END;

    BEGIN TRY
        INSERT study.Study_Entry (Study_Participant_ID, Source_Entry_ID, Entry_Created_At,
                                  Entry_Text, Safety_Tier_At_Capture, Is_Restricted)
        VALUES (@PID, @Source_Entry_ID, @Entry_Created_At,
                CASE WHEN @Keep_Text = 1 THEN @Entry_Text END,
                @Safety_Tier_At_Capture,
                CASE WHEN @Safety_Tier_At_Capture >= 4 THEN 1 ELSE 0 END);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Study_Entry_ID, Detail)
        VALUES ('INGEST', @PID, SCOPE_IDENTITY(), CASE WHEN @Keep_Text = 1 THEN N'text kept' ELSE N'no text' END);
        SELECT CASE WHEN @Keep_Text = 1 THEN 'INGESTED' ELSE 'INGESTED_NO_TEXT' END AS Ingest_Status;
    END TRY
    BEGIN CATCH
        -- The entry table is RLS-filtered for the app, so idempotency
        -- relies on the unique index rather than an existence check.
        IF ERROR_NUMBER() IN (2601, 2627)
            SELECT 'DUPLICATE' AS Ingest_Status;
        ELSE
            THROW;
    END CATCH;
END;
GO

/* ---------------------------------------------------------------------
   3. Revoking B3 erases the kept text too.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Revoke_Text_Retention
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Revoking text retention must be run by a study coordinator.', 1;

    BEGIN TRAN;
        DELETE g FROM study.Gold_Label g
          JOIN study.Study_Entry e ON e.Study_Entry_ID = g.Study_Entry_ID
         WHERE e.Study_Participant_ID = @Study_Participant_ID;

        DELETE a FROM study.Annotation a
          JOIN study.Study_Entry e ON e.Study_Entry_ID = a.Study_Entry_ID
         WHERE e.Study_Participant_ID = @Study_Participant_ID;

        DELETE d FROM study.Entry_Deid d
          JOIN study.Study_Entry e ON e.Study_Entry_ID = d.Study_Entry_ID
         WHERE e.Study_Participant_ID = @Study_Participant_ID;

        UPDATE study.Study_Entry SET Entry_Text = NULL
         WHERE Study_Participant_ID = @Study_Participant_ID AND Entry_Text IS NOT NULL;

        UPDATE study.Study_Participant
           SET Text_Retention_At = NULL, Text_Retention_Version = NULL
         WHERE Study_Participant_ID = @Study_Participant_ID;

        INSERT study.Access_Audit (Action, Study_Participant_ID)
        VALUES ('REVOKE_TEXT_RETENTION', @Study_Participant_ID);
    COMMIT;
END;
GO

/* ---------------------------------------------------------------------
   4. De-identification only from kept text.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Get_Entries_For_Deid
    @Max_Rows INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Out TABLE (Study_Entry_ID BIGINT PRIMARY KEY, Study_Participant_ID INT);

    INSERT @Out (Study_Entry_ID, Study_Participant_ID)
    SELECT TOP (@Max_Rows) e.Study_Entry_ID, e.Study_Participant_ID
    FROM study.Study_Entry e
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = e.Study_Participant_ID
    WHERE sp.Withdrawn_At IS NULL
      AND sp.Text_Retention_At IS NOT NULL
      AND e.Entry_Text IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM study.Entry_Deid d WHERE d.Study_Entry_ID = e.Study_Entry_ID)
    ORDER BY e.Study_Entry_ID;

    INSERT study.Access_Audit (Action, Study_Participant_ID, Study_Entry_ID)
    SELECT 'DEID_READ', Study_Participant_ID, Study_Entry_ID FROM @Out;

    SELECT e.Study_Entry_ID, e.Study_Participant_ID, e.Entry_Created_At, e.Entry_Text,
           pl.Date_Offset_Days
    FROM @Out o
    JOIN study.Study_Entry e ON e.Study_Entry_ID = o.Study_Entry_ID
    LEFT JOIN study.Participant_Link pl ON pl.Study_Participant_ID = e.Study_Participant_ID
    ORDER BY e.Study_Entry_ID;
END;
GO

CREATE OR ALTER PROCEDURE study.usp_Save_Deid_Text
    @Study_Entry_ID BIGINT,
    @Deid_Text NVARCHAR(MAX),
    @Pipeline_Version NVARCHAR(40)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @PID INT, @Retention DATETIME2(0), @Withdrawn DATETIME2(0), @Has_Text BIT;

    SELECT @PID = e.Study_Participant_ID, @Retention = sp.Text_Retention_At, @Withdrawn = sp.Withdrawn_At,
           @Has_Text = CASE WHEN e.Entry_Text IS NULL THEN 0 ELSE 1 END
    FROM study.Study_Entry e
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = e.Study_Participant_ID
    WHERE e.Study_Entry_ID = @Study_Entry_ID;

    IF @PID IS NULL THROW 51002, N'Entry not found or not accessible.', 1;
    IF @Withdrawn IS NOT NULL THROW 51022, N'The participant has withdrawn.', 1;
    IF @Retention IS NULL THROW 51023, N'The participant hasn''t given B3 permission to keep text.', 1;
    IF @Has_Text = 0 THROW 51029, N'No text was kept for this entry, so there is nothing to de-identify.', 1;
    IF LEN(LTRIM(RTRIM(ISNULL(@Deid_Text, N'')))) = 0 THROW 51024, N'De-identified text is empty.', 1;
    IF EXISTS (SELECT 1 FROM study.Annotation WHERE Study_Entry_ID = @Study_Entry_ID)
        THROW 51025, N'This entry has already been rated, so its de-identified text can''t be replaced.', 1;

    BEGIN TRAN;
        MERGE study.Entry_Deid AS t
        USING (SELECT @Study_Entry_ID AS Study_Entry_ID) AS s
           ON t.Study_Entry_ID = s.Study_Entry_ID
        WHEN MATCHED THEN UPDATE SET
            Deid_Text = @Deid_Text, Pipeline_Version = @Pipeline_Version, Saved_At = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (Study_Entry_ID, Deid_Text, Pipeline_Version)
            VALUES (@Study_Entry_ID, @Deid_Text, @Pipeline_Version);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Study_Entry_ID, Detail)
        VALUES ('DEID_SAVE', @PID, @Study_Entry_ID, @Pipeline_Version);
    COMMIT;
END;
GO

/* ---------------------------------------------------------------------
   Security policy (as in study_patch_2026-10-17_rating.sql)
   --------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'pol_study_access'
               AND schema_id = SCHEMA_ID(N'study_sec'))
    CREATE SECURITY POLICY study_sec.pol_study_access
        ADD FILTER PREDICATE study_sec.fn_entry_access (Study_Participant_ID, Is_Restricted, Study_Entry_ID) ON study.Study_Entry,
        ADD FILTER PREDICATE study_sec.fn_annotation_access (Annotator_ID, Study_Entry_ID) ON study.Annotation,
        ADD BLOCK  PREDICATE study_sec.fn_annotation_write (Annotator_ID) ON study.Annotation AFTER INSERT,
        ADD BLOCK  PREDICATE study_sec.fn_annotation_write (Annotator_ID) ON study.Annotation AFTER UPDATE
    WITH (STATE = ON, SCHEMABINDING = ON);
GO

/* Check: every row should say ok. */
SELECT 'Study_Entry.Entry_Text optional' AS name,
       CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'study.Study_Entry')
                         AND name = N'Entry_Text' AND is_nullable = 1) THEN 'ok' ELSE 'MISSING' END AS status
UNION ALL
SELECT 'usp_Ingest_Entry keeps text only with B3',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Ingest_Entry')) LIKE N'%@Keep_Text%' THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'usp_Revoke_Text_Retention erases kept text',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Revoke_Text_Retention')) LIKE N'%SET Entry_Text = NULL%' THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'pol_study_access',
       CASE WHEN EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'pol_study_access' AND is_enabled = 1)
            THEN 'ok' ELSE 'MISSING' END;
GO
