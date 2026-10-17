/* =====================================================================
   UCANRR study schema: patch 2026-10-17, the protocol §10.2 rating
   Run as the server admin AFTER study_schema_2026-10-06.sql and
   study_patch_2026-10-07.sql. Safe to re-run.

   Protocol v1.0 §9.4 and §9.6: no one on the study team reads entry
   text as written, and the treating therapist never sees entries.
   Attachment B3: text is kept only with the participant's permission,
   de-identified, and used for blind rating after the study (§10.2).
   This patch turns the annotation tables into that rating:

     1. study.Study_Close: the coordinator closes the study once. Rating
        opens only after that, and ingestion stops.
     2. Study_Participant.Text_Retention_At / _Version: the participant's
        B3 permission. Revoking it deletes the participant's de-identified
        text, ratings and gold labels.
     3. study.Entry_Deid: de-identified text, written by the
        de-identification pipeline (new role study_deid). Raters and the
        adjudicator read only this, never Entry_Text.
     4. Annotator types RATER and ADJUDICATOR. The treating therapist is
        no longer an annotator type; the API also refuses to assign a
        couple's own therapist.
     5. Row-level security: a rater or adjudicator sees an entry only
        after close, with B3 permission and de-identified text. The
        pipeline role passes the entry and annotation filters.
     6. The rating procedures return no app scores (Safety_Tier_At_Capture),
        no entry dates and no raw text.
     7. vw_Label_Pairs pairs the two raters; vw_Annotation_Progress also
        counts rateable entries.
     8. Withdrawal also deletes de-identified text.

   Not changed here (separate build items): entry text is still copied
   into Study_Entry for every enrolled participant, and the coordinator
   role can still SELECT Entry_Text (ciphertext once Always Encrypted is on).
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   0. Guard: treating-therapist annotators can't be carried over. Their
      labels were made on raw text and are ruled out by the protocol.
      Stop the whole script, without changing anything, if any exist.
   --------------------------------------------------------------------- */
IF EXISTS (SELECT 1 FROM study.Annotator WHERE Annotator_Type = 'TREATING_THERAPIST')
BEGIN
    RAISERROR(N'STOPPED: study.Annotator has TREATING_THERAPIST rows. The protocol rules out therapist labels. Delete their gold labels, annotations, assignments and annotator rows (see the note at the end of this script), then run it again. Nothing was changed. The errors listed after this one come from the skipped sections and can be ignored.', 16, 1);
    SET NOEXEC ON;
END;
GO

/* ---------------------------------------------------------------------
   1. Drop the security policy so its predicate functions can change.
      It is recreated in section 6.
   --------------------------------------------------------------------- */
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'pol_study_access'
           AND schema_id = SCHEMA_ID(N'study_sec'))
    DROP SECURITY POLICY study_sec.pol_study_access;
GO

/* ---------------------------------------------------------------------
   2. Roles, tables and columns
   --------------------------------------------------------------------- */
-- The de-identification pipeline's identity: reads raw text of entries
-- with B3 permission and writes the de-identified copy. Nothing else.
IF DATABASE_PRINCIPAL_ID(N'study_deid') IS NULL CREATE ROLE study_deid;
GO

-- Annotator types: two raters and an adjudicator.
IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_study_Annotator_Type')
    ALTER TABLE study.Annotator DROP CONSTRAINT CK_study_Annotator_Type;
GO
UPDATE study.Annotator SET Annotator_Type = 'RATER' WHERE Annotator_Type = 'SECOND_LABELER';
ALTER TABLE study.Annotator ADD CONSTRAINT CK_study_Annotator_Type
    CHECK (Annotator_Type IN ('RATER', 'ADJUDICATOR'));
GO

-- B3: permission to keep this participant's text, de-identified.
IF COL_LENGTH('study.Study_Participant', 'Text_Retention_At') IS NULL
    ALTER TABLE study.Study_Participant ADD
        Text_Retention_At       DATETIME2(0) NULL,
        Text_Retention_Version  NVARCHAR(20) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_study_SP_TextRetention')
    ALTER TABLE study.Study_Participant ADD CONSTRAINT CK_study_SP_TextRetention
        CHECK ((Text_Retention_At IS NULL AND Text_Retention_Version IS NULL)
            OR (Text_Retention_At IS NOT NULL AND Text_Retention_Version IS NOT NULL));
GO

-- Study close. One row, written once, never changed.
IF OBJECT_ID(N'study.Study_Close', N'U') IS NULL
    CREATE TABLE study.Study_Close (
        Study_Close_ID     TINYINT      NOT NULL CONSTRAINT PK_study_Study_Close PRIMARY KEY
            CONSTRAINT CK_study_Close_One CHECK (Study_Close_ID = 1),
        Closed_At          DATETIME2(0) NOT NULL CONSTRAINT DF_study_Close_At DEFAULT SYSUTCDATETIME(),
        Closed_By          SYSNAME      NOT NULL CONSTRAINT DF_study_Close_By DEFAULT USER_NAME(),
        Closed_By_User_ID  INT          NULL
            CONSTRAINT DF_study_Close_Actor DEFAULT (TRY_CONVERT(INT, SESSION_CONTEXT(N'actor_user_id')))
    );
GO
CREATE OR ALTER TRIGGER study.trg_Study_Close_Final
ON study.Study_Close
INSTEAD OF UPDATE, DELETE
AS
BEGIN
    THROW 51021, N'The study close is final and can''t be changed.', 1;
END;
GO

/* De-identified copies. Raters read only this text.
   Deid_Text: encrypt with Always Encrypted (randomized) in production,
   as for Study_Entry.Entry_Text. De-identified free text can still
   identify someone in a small community. */
IF OBJECT_ID(N'study.Entry_Deid', N'U') IS NULL
    CREATE TABLE study.Entry_Deid (
        Study_Entry_ID    BIGINT        NOT NULL CONSTRAINT PK_study_Entry_Deid PRIMARY KEY
            CONSTRAINT FK_study_ED_Entry REFERENCES study.Study_Entry (Study_Entry_ID),
        Deid_Text         NVARCHAR(MAX) NOT NULL,
        /* PRODUCTION: replace the line above with
           Deid_Text NVARCHAR(MAX) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = CEK_study,
               ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL, */
        Pipeline_Version  NVARCHAR(40)  NOT NULL,
        Saved_At          DATETIME2(0)  NOT NULL CONSTRAINT DF_study_ED_Saved DEFAULT SYSUTCDATETIME()
    );
GO

-- Audit actions for the new procedures.
IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_study_Audit_Action')
    ALTER TABLE study.Access_Audit DROP CONSTRAINT CK_study_Audit_Action;
GO
ALTER TABLE study.Access_Audit ADD CONSTRAINT CK_study_Audit_Action CHECK (Action IN (
    'SESSION_START','SESSION_DENIED','VIEW_QUEUE','VIEW_ENTRY','VIEW_ENTRY_DENIED',
    'SAVE_ANNOTATION','SAVE_DENIED','SAVE_GOLD','INGEST','INGEST_SKIPPED',
    'ENROLL','ASSIGN','REVOKE_ASSIGNMENT','REGISTER_AGREEMENT','REVOKE_AGREEMENT',
    'REGISTER_ANNOTATOR','PAUSE','RESUME','WITHDRAW',
    'CLOSE_STUDY','RECORD_TEXT_RETENTION','REVOKE_TEXT_RETENTION','DEID_READ','DEID_SAVE'));
GO

/* ---------------------------------------------------------------------
   3. Row-level security predicate
      Entry visibility: the coordinator; the de-identification pipeline;
      or, after the study is closed, a rater or adjudicator with a live
      agreement and an active assignment to a non-withdrawn participant
      who gave B3 permission, for entries that have de-identified text.
      Restricted entries still need Can_View_Restricted.
   --------------------------------------------------------------------- */
CREATE OR ALTER FUNCTION study_sec.fn_entry_access
    (@Study_Participant_ID INT, @Is_Restricted BIT, @Study_Entry_ID BIGINT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS ok
    WHERE IS_MEMBER(N'study_coordinator') = 1
       OR IS_MEMBER(N'study_deid') = 1
       OR (    EXISTS (SELECT 1 FROM study.Study_Close)
           AND EXISTS (SELECT 1 FROM study.Entry_Deid d WHERE d.Study_Entry_ID = @Study_Entry_ID)
           AND EXISTS (
                SELECT 1
                FROM study.Annotator_Assignment aa
                JOIN study.Annotator a          ON a.Annotator_ID = aa.Annotator_ID
                JOIN study.Privacy_Agreement pa ON pa.Agreement_ID = a.Agreement_ID
                JOIN study.Study_Participant sp ON sp.Study_Participant_ID = aa.Study_Participant_ID
                WHERE aa.Study_Participant_ID = @Study_Participant_ID
                  AND aa.Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT)
                  AND aa.Revoked_At IS NULL
                  AND a.Is_Active = 1
                  AND a.Annotator_Type IN ('RATER', 'ADJUDICATOR')
                  AND pa.Revoked_At IS NULL
                  AND (pa.Expires_At IS NULL OR pa.Expires_At > SYSUTCDATETIME())
                  AND sp.Withdrawn_At IS NULL
                  AND sp.Text_Retention_At IS NOT NULL
                  AND (@Is_Restricted = 0 OR a.Can_View_Restricted = 1)));
GO

/* Annotation visibility: as before (blind rating: raters see only their
   own; the adjudicator sees all on entries it can see), plus the
   de-identification pipeline, so usp_Save_Deid_Text can tell that an
   entry has been rated. study_deid has no direct table access and no
   procedure returns ratings to it. */
CREATE OR ALTER FUNCTION study_sec.fn_annotation_access (@Annotator_ID INT, @Study_Entry_ID BIGINT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS ok
    WHERE IS_MEMBER(N'study_coordinator') = 1
       OR IS_MEMBER(N'study_deid') = 1
       OR @Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT)
       OR EXISTS (
            SELECT 1
            FROM study.Annotator a
            JOIN study.Study_Entry e ON e.Study_Entry_ID = @Study_Entry_ID   -- Study_Entry's own filter applies
            WHERE a.Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT)
              AND a.Annotator_Type = 'ADJUDICATOR'
              AND a.Is_Active = 1);
GO

/* ---------------------------------------------------------------------
   4. Procedures used by the rating pages (study_app)
   --------------------------------------------------------------------- */

/* Whether rating is open. Readable before any annotator check, so the
   page can say "rating opens after the study closes". */
CREATE OR ALTER PROCEDURE study.usp_Get_Study_Status
AS
BEGIN
    SET NOCOUNT ON;
    SELECT (SELECT Closed_At FROM study.Study_Close) AS Closed_At;
END;
GO

/* A rater's queue: entries they may rate. No dates and no app scores. */
CREATE OR ALTER PROCEDURE study.usp_Get_Annotation_Queue
    @Include_Done BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Annotator
                   WHERE Annotator_ID = @Me AND Annotator_Type = 'RATER' AND Is_Active = 1)
        THROW 51005, N'Only an active rater has a rating queue.', 1;

    INSERT study.Access_Audit (Annotator_ID, Action) VALUES (@Me, 'VIEW_QUEUE');

    SELECT e.Study_Entry_ID,
           e.Study_Participant_ID,
           CAST(CASE WHEN a.Annotation_ID IS NULL THEN 0 ELSE 1 END AS BIT) AS Has_My_Annotation
    FROM study.Study_Entry e                      -- RLS limits rows
    LEFT JOIN study.Annotation a
           ON a.Study_Entry_ID = e.Study_Entry_ID
          AND a.Annotator_ID = @Me
          AND a.Is_Current = 1
    WHERE @Include_Done = 1 OR a.Annotation_ID IS NULL
    ORDER BY Has_My_Annotation, e.Study_Participant_ID, e.Study_Entry_ID;
END;
GO

/* One entry for a rater or the adjudicator: the de-identified text only. */
CREATE OR ALTER PROCEDURE study.usp_Get_Entry
    @Study_Entry_ID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    DECLARE @PID INT;
    SELECT @PID = e.Study_Participant_ID
    FROM study.Study_Entry e                      -- RLS limits rows
    JOIN study.Entry_Deid d ON d.Study_Entry_ID = e.Study_Entry_ID
    WHERE e.Study_Entry_ID = @Study_Entry_ID;

    IF @PID IS NULL
    BEGIN
        -- Either it doesn't exist or RLS hides it. Both are worth reviewing.
        INSERT study.Access_Audit (Annotator_ID, Action, Study_Entry_ID)
        VALUES (@Me, 'VIEW_ENTRY_DENIED', @Study_Entry_ID);
        RETURN;  -- empty result
    END;

    INSERT study.Access_Audit (Annotator_ID, Action, Study_Participant_ID, Study_Entry_ID)
    VALUES (@Me, 'VIEW_ENTRY', @PID, @Study_Entry_ID);

    SELECT e.Study_Entry_ID, e.Study_Participant_ID, d.Deid_Text
    FROM study.Study_Entry e
    JOIN study.Entry_Deid d ON d.Study_Entry_ID = e.Study_Entry_ID
    WHERE e.Study_Entry_ID = @Study_Entry_ID;
END;
GO

CREATE OR ALTER PROCEDURE study.usp_Save_Annotation
    @Study_Entry_ID BIGINT,
    @IsNeglect BIT, @IsRepair BIT, @IsBid BIT, @IsCare BIT, @IsShared BIT,
    @Sentiment SMALLINT,
    @Safety_Tier TINYINT,
    @Notes NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Annotator
                   WHERE Annotator_ID = @Me AND Annotator_Type = 'RATER' AND Is_Active = 1)
        THROW 51005, N'Only an active rater can save ratings.', 1;

    -- Foreign-key checks are not filtered by RLS, so check visibility explicitly.
    IF NOT EXISTS (SELECT 1 FROM study.Study_Entry WHERE Study_Entry_ID = @Study_Entry_ID)
    BEGIN
        INSERT study.Access_Audit (Annotator_ID, Action, Study_Entry_ID, Detail)
        VALUES (@Me, 'SAVE_DENIED', @Study_Entry_ID, N'Entry not visible');
        THROW 51002, N'Entry not found or not accessible.', 1;
    END;

    IF @Sentiment NOT BETWEEN -10 AND 10 THROW 51003, N'Sentiment must be between -10 and 10.', 1;
    IF @Safety_Tier NOT BETWEEN 1 AND 5  THROW 51004, N'Safety tier must be between 1 and 5.', 1;

    BEGIN TRAN;
        DECLARE @Next INT =
            ISNULL((SELECT MAX(Version_No) FROM study.Annotation WITH (UPDLOCK, HOLDLOCK)
                    WHERE Study_Entry_ID = @Study_Entry_ID AND Annotator_ID = @Me), 0) + 1;

        UPDATE study.Annotation
           SET Is_Current = 0
         WHERE Study_Entry_ID = @Study_Entry_ID AND Annotator_ID = @Me AND Is_Current = 1;

        INSERT study.Annotation (Study_Entry_ID, Annotator_ID, Version_No, Is_Current,
                                 IsNeglect, IsRepair, IsBid, IsCare, IsShared,
                                 Sentiment, Safety_Tier, Notes)
        VALUES (@Study_Entry_ID, @Me, @Next, 1,
                @IsNeglect, @IsRepair, @IsBid, @IsCare, @IsShared,
                @Sentiment, @Safety_Tier, @Notes);

        INSERT study.Access_Audit (Annotator_ID, Action, Study_Entry_ID, Detail)
        VALUES (@Me, 'SAVE_ANNOTATION', @Study_Entry_ID, CONCAT(N'version ', @Next));
    COMMIT;

    SELECT @Next AS Version_No;
END;
GO

/* Adjudication queue: as before, minus entry dates and app scores. */
CREATE OR ALTER PROCEDURE study.usp_Get_Adjudication_Queue
    @Include_Done BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Annotator
                   WHERE Annotator_ID = @Me AND Annotator_Type = 'ADJUDICATOR' AND Is_Active = 1)
        THROW 51005, N'Only an active adjudicator can view the adjudication queue.', 1;

    INSERT study.Access_Audit (Annotator_ID, Action, Detail) VALUES (@Me, 'VIEW_QUEUE', N'adjudication');

    SELECT q.Study_Entry_ID, q.Study_Participant_ID,
           q.Labels, q.Min_Tier, q.Max_Tier, q.Is_Unanimous, q.Has_Gold
    FROM (
        SELECT e.Study_Entry_ID, e.Study_Participant_ID,
               x.Labels, x.Min_Tier, x.Max_Tier,
               CAST(CASE WHEN x.Min_Neglect = x.Max_Neglect AND x.Min_Repair = x.Max_Repair
                          AND x.Min_Bid = x.Max_Bid AND x.Min_Care = x.Max_Care
                          AND x.Min_Shared = x.Max_Shared AND x.Min_Sentiment = x.Max_Sentiment
                          AND x.Min_Tier = x.Max_Tier
                         THEN 1 ELSE 0 END AS BIT) AS Is_Unanimous,
               CAST(CASE WHEN g.Study_Entry_ID IS NULL THEN 0 ELSE 1 END AS BIT) AS Has_Gold
        FROM study.Study_Entry e                      -- RLS limits rows
        CROSS APPLY (
            SELECT COUNT(*) AS Labels,
                   MIN(a.Safety_Tier) AS Min_Tier,           MAX(a.Safety_Tier) AS Max_Tier,
                   MIN(a.Sentiment)   AS Min_Sentiment,      MAX(a.Sentiment)   AS Max_Sentiment,
                   MIN(CAST(a.IsNeglect AS INT)) AS Min_Neglect, MAX(CAST(a.IsNeglect AS INT)) AS Max_Neglect,
                   MIN(CAST(a.IsRepair  AS INT)) AS Min_Repair,  MAX(CAST(a.IsRepair  AS INT)) AS Max_Repair,
                   MIN(CAST(a.IsBid     AS INT)) AS Min_Bid,     MAX(CAST(a.IsBid     AS INT)) AS Max_Bid,
                   MIN(CAST(a.IsCare    AS INT)) AS Min_Care,    MAX(CAST(a.IsCare    AS INT)) AS Max_Care,
                   MIN(CAST(a.IsShared  AS INT)) AS Min_Shared,  MAX(CAST(a.IsShared  AS INT)) AS Max_Shared
            FROM study.Annotation a
            WHERE a.Study_Entry_ID = e.Study_Entry_ID AND a.Is_Current = 1 AND a.Annotator_ID <> @Me
        ) x
        LEFT JOIN study.Gold_Label g ON g.Study_Entry_ID = e.Study_Entry_ID
        WHERE x.Labels >= 2
    ) q
    WHERE @Include_Done = 1 OR q.Has_Gold = 0
    ORDER BY q.Has_Gold, q.Is_Unanimous,
             CASE WHEN q.Min_Tier < 4 AND q.Max_Tier >= 4 THEN 0 ELSE 1 END,
             q.Max_Tier DESC, q.Study_Participant_ID, q.Study_Entry_ID;
END;
GO

/* ---------------------------------------------------------------------
   5. Pipeline, coordinator and lifecycle procedures
   --------------------------------------------------------------------- */

/* The de-identification pipeline's input: raw text of entries from
   non-withdrawn participants with B3 permission that have no
   de-identified copy yet. Date_Offset_Days is for the date shift. */
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

/* Save the de-identified copy. Replacing it is refused once anyone has
   rated the entry, so every rating refers to the text the rater saw. */
CREATE OR ALTER PROCEDURE study.usp_Save_Deid_Text
    @Study_Entry_ID BIGINT,
    @Deid_Text NVARCHAR(MAX),
    @Pipeline_Version NVARCHAR(40)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @PID INT, @Retention DATETIME2(0), @Withdrawn DATETIME2(0);

    SELECT @PID = e.Study_Participant_ID, @Retention = sp.Text_Retention_At, @Withdrawn = sp.Withdrawn_At
    FROM study.Study_Entry e
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = e.Study_Participant_ID
    WHERE e.Study_Entry_ID = @Study_Entry_ID;

    IF @PID IS NULL THROW 51002, N'Entry not found or not accessible.', 1;
    IF @Withdrawn IS NOT NULL THROW 51022, N'The participant has withdrawn.', 1;
    IF @Retention IS NULL THROW 51023, N'The participant hasn''t given B3 permission to keep text.', 1;
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

/* Close the study. Final: rating opens and ingestion stops. */
CREATE OR ALTER PROCEDURE study.usp_Close_Study
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM study.Study_Close)
        THROW 51021, N'The study is already closed.', 1;
    INSERT study.Study_Close (Study_Close_ID) VALUES (1);
    INSERT study.Access_Audit (Action) VALUES ('CLOSE_STUDY');
    SELECT Closed_At FROM study.Study_Close;
END;
GO

/* Record B3 permission for one participant (one partner). */
CREATE OR ALTER PROCEDURE study.usp_Record_Text_Retention
    @Study_Participant_ID INT,
    @Authorized_At DATETIME2(0),
    @Version NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM study.Study_Participant
                   WHERE Study_Participant_ID = @Study_Participant_ID AND Withdrawn_At IS NULL)
        THROW 51022, N'Participant not found or withdrawn.', 1;
    IF @Authorized_At > SYSUTCDATETIME()
        THROW 51026, N'The B3 signing time can''t be in the future.', 1;
    IF LEN(LTRIM(RTRIM(ISNULL(@Version, N'')))) = 0
        THROW 51027, N'Give the B3 form version.', 1;

    UPDATE study.Study_Participant
       SET Text_Retention_At = @Authorized_At, Text_Retention_Version = @Version
     WHERE Study_Participant_ID = @Study_Participant_ID;

    INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
    VALUES ('RECORD_TEXT_RETENTION', @Study_Participant_ID, @Version);
END;
GO

/* Revoke B3 permission: deletes the participant's de-identified text,
   ratings and gold labels. Must be run by a coordinator. */
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

        UPDATE study.Study_Participant
           SET Text_Retention_At = NULL, Text_Retention_Version = NULL
         WHERE Study_Participant_ID = @Study_Participant_ID;

        INSERT study.Access_Audit (Action, Study_Participant_ID)
        VALUES ('REVOKE_TEXT_RETENTION', @Study_Participant_ID);
    COMMIT;
END;
GO

/* Ingestion: as before, and stops once the study is closed. */
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
    DECLARE @PID INT, @Consent DATETIME2(0), @Paused DATETIME2(0), @Withdrawn DATETIME2(0), @Status VARCHAR(30);

    SELECT @PID = sp.Study_Participant_ID, @Consent = sp.Research_Consent_At,
           @Paused = sp.Research_Paused_At, @Withdrawn = sp.Withdrawn_At
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

    BEGIN TRY
        INSERT study.Study_Entry (Study_Participant_ID, Source_Entry_ID, Entry_Created_At,
                                  Entry_Text, Safety_Tier_At_Capture, Is_Restricted)
        VALUES (@PID, @Source_Entry_ID, @Entry_Created_At, @Entry_Text, @Safety_Tier_At_Capture,
                CASE WHEN @Safety_Tier_At_Capture >= 4 THEN 1 ELSE 0 END);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Study_Entry_ID)
        VALUES ('INGEST', @PID, SCOPE_IDENTITY());
        SELECT 'INGESTED' AS Ingest_Status;
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

/* Withdrawal: as before, and also deletes de-identified text. */
CREATE OR ALTER PROCEDURE study.usp_Withdraw_Participant
    @Study_Participant_ID INT,
    @Delete_Linkage BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Withdrawal must be run by a study coordinator.', 1;

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

        DELETE FROM study.Study_Entry WHERE Study_Participant_ID = @Study_Participant_ID;

        UPDATE study.Annotator_Assignment SET Revoked_At = SYSUTCDATETIME()
         WHERE Study_Participant_ID = @Study_Participant_ID AND Revoked_At IS NULL;

        UPDATE study.Study_Participant SET Withdrawn_At = SYSUTCDATETIME()
         WHERE Study_Participant_ID = @Study_Participant_ID;

        IF @Delete_Linkage = 1
            DELETE FROM study.Participant_Link WHERE Study_Participant_ID = @Study_Participant_ID;

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        VALUES ('WITHDRAW', @Study_Participant_ID, CASE WHEN @Delete_Linkage = 1 THEN N'linkage deleted' ELSE N'linkage kept' END);
    COMMIT;
END;
GO

/* ---------------------------------------------------------------------
   6. Security policy (recreated with the new entry predicate)
   --------------------------------------------------------------------- */
CREATE SECURITY POLICY study_sec.pol_study_access
    ADD FILTER PREDICATE study_sec.fn_entry_access (Study_Participant_ID, Is_Restricted, Study_Entry_ID) ON study.Study_Entry,
    ADD FILTER PREDICATE study_sec.fn_annotation_access (Annotator_ID, Study_Entry_ID) ON study.Annotation,
    ADD BLOCK  PREDICATE study_sec.fn_annotation_write (Annotator_ID) ON study.Annotation AFTER INSERT,
    ADD BLOCK  PREDICATE study_sec.fn_annotation_write (Annotator_ID) ON study.Annotation AFTER UPDATE
WITH (STATE = ON, SCHEMABINDING = ON);
GO

/* ---------------------------------------------------------------------
   7. Views (coordinator)
   --------------------------------------------------------------------- */

/* Inter-rater pairs: one row per entry with current ratings from two
   raters. R1 is the rater with the lower Annotator_ID. Labels only. */
CREATE OR ALTER VIEW study.vw_Label_Pairs
AS
WITH cur AS (
    SELECT a.Study_Entry_ID,
           a.IsNeglect, a.IsRepair, a.IsBid, a.IsCare, a.IsShared, a.Sentiment, a.Safety_Tier,
           ROW_NUMBER() OVER (PARTITION BY a.Study_Entry_ID ORDER BY a.Annotator_ID) AS rn
    FROM study.Annotation a
    JOIN study.Annotator an ON an.Annotator_ID = a.Annotator_ID
    WHERE a.Is_Current = 1
      AND an.Annotator_Type = 'RATER'
)
SELECT e.Study_Entry_ID,
       e.Study_Participant_ID,
       r1.IsNeglect AS R1_IsNeglect, r1.IsRepair AS R1_IsRepair, r1.IsBid AS R1_IsBid,
       r1.IsCare    AS R1_IsCare,    r1.IsShared AS R1_IsShared,
       r1.Sentiment AS R1_Sentiment, r1.Safety_Tier AS R1_Safety_Tier,
       r2.IsNeglect AS R2_IsNeglect, r2.IsRepair AS R2_IsRepair, r2.IsBid AS R2_IsBid,
       r2.IsCare    AS R2_IsCare,    r2.IsShared AS R2_IsShared,
       r2.Sentiment AS R2_Sentiment, r2.Safety_Tier AS R2_Safety_Tier
FROM study.Study_Entry e
JOIN cur r1 ON r1.Study_Entry_ID = e.Study_Entry_ID AND r1.rn = 1
JOIN cur r2 ON r2.Study_Entry_ID = e.Study_Entry_ID AND r2.rn = 2;
GO

/* Progress per participant. Rateable: B3 permission and de-identified text. */
CREATE OR ALTER VIEW study.vw_Annotation_Progress
AS
SELECT e.Study_Participant_ID,
       COUNT(*)                                            AS Entries,
       SUM(CAST(e.Is_Restricted AS INT))                   AS Restricted_Entries,
       SUM(CASE WHEN d.Study_Entry_ID IS NOT NULL AND sp.Text_Retention_At IS NOT NULL
                THEN 1 ELSE 0 END)                         AS Rateable_Entries,
       SUM(CASE WHEN x.N >= 1 THEN 1 ELSE 0 END)           AS Labeled_Once,
       SUM(CASE WHEN x.N >= 2 THEN 1 ELSE 0 END)           AS Labeled_Twice,
       SUM(CASE WHEN g.Study_Entry_ID IS NULL THEN 0 ELSE 1 END) AS Gold_Final
FROM study.Study_Entry e
JOIN study.Study_Participant sp ON sp.Study_Participant_ID = e.Study_Participant_ID
LEFT JOIN study.Entry_Deid d ON d.Study_Entry_ID = e.Study_Entry_ID
OUTER APPLY (SELECT COUNT(*) AS N FROM study.Annotation a
             JOIN study.Annotator an ON an.Annotator_ID = a.Annotator_ID
             WHERE a.Study_Entry_ID = e.Study_Entry_ID AND a.Is_Current = 1
               AND an.Annotator_Type = 'RATER') x
LEFT JOIN study.Gold_Label g ON g.Study_Entry_ID = e.Study_Entry_ID
GROUP BY e.Study_Participant_ID;
GO

/* ---------------------------------------------------------------------
   8. Permissions
   --------------------------------------------------------------------- */
DENY SELECT, INSERT, UPDATE, DELETE ON SCHEMA::study TO study_deid;
DENY SELECT, INSERT, UPDATE, DELETE ON SCHEMA::study_sec TO study_deid;

GRANT EXECUTE ON study.usp_Get_Study_Status        TO study_app;
GRANT EXECUTE ON study.usp_Get_Study_Status        TO study_coordinator;
GRANT EXECUTE ON study.usp_Close_Study             TO study_coordinator;
GRANT EXECUTE ON study.usp_Record_Text_Retention   TO study_coordinator;
GRANT EXECUTE ON study.usp_Revoke_Text_Retention   TO study_coordinator;
GRANT EXECUTE ON study.usp_Get_Entries_For_Deid    TO study_deid;
GRANT EXECUTE ON study.usp_Save_Deid_Text          TO study_deid;
-- The coordinator's schema-level SELECT covers the new tables and views.
DENY UPDATE, DELETE ON study.Study_Close TO study_coordinator;
GO

/* Map the pipeline's identity (run once per environment; edit the name):
   CREATE USER [ucanrr-deid-pipeline] FROM EXTERNAL PROVIDER;
   ALTER ROLE study_deid ADD MEMBER [ucanrr-deid-pipeline];
*/

/* Check: every row should say ok. */
SELECT v.name,
       CASE WHEN OBJECT_ID('study.' + v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('Study_Close'), ('Entry_Deid'), ('usp_Get_Study_Status'), ('usp_Close_Study'),
             ('usp_Record_Text_Retention'), ('usp_Revoke_Text_Retention'),
             ('usp_Get_Entries_For_Deid'), ('usp_Save_Deid_Text')) v(name)
UNION ALL
SELECT 'Study_Participant.Text_Retention_At',
       CASE WHEN COL_LENGTH('study.Study_Participant', 'Text_Retention_At') IS NULL THEN 'MISSING' ELSE 'ok' END
UNION ALL
SELECT 'pol_study_access',
       CASE WHEN EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'pol_study_access' AND is_enabled = 1)
            THEN 'ok' ELSE 'MISSING' END;
GO

SET NOEXEC OFF;
GO

/* Removing treating-therapist annotators (only if section 0 stopped the
   script). Review first: this deletes their labels.
   DECLARE @T TABLE (Annotator_ID INT);
   INSERT @T SELECT Annotator_ID FROM study.Annotator WHERE Annotator_Type = 'TREATING_THERAPIST';
   DELETE g FROM study.Gold_Label g WHERE EXISTS (SELECT 1 FROM study.Annotation a JOIN @T t ON t.Annotator_ID = a.Annotator_ID WHERE a.Study_Entry_ID = g.Study_Entry_ID);
   DELETE a FROM study.Annotation a JOIN @T t ON t.Annotator_ID = a.Annotator_ID;
   DELETE aa FROM study.Annotator_Assignment aa JOIN @T t ON t.Annotator_ID = aa.Annotator_ID;
   DELETE an FROM study.Annotator an JOIN @T t ON t.Annotator_ID = an.Annotator_ID;
*/
