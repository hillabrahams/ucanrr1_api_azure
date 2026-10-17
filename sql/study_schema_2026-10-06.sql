/* =====================================================================
   UCANRR Feasibility Study: therapist annotation scaffolding
   Target:   the existing UCANRR production Azure SQL database
   Schemas:  [study]      data + procedures
             [study_sec]  row-level-security predicate functions
   Version:  0.1  (2026-10-02)
   Status:   NOT YET EXECUTED. Run on a dev copy of the database first,
             then run test_study_annotation.sql against that copy.

   Design summary
   - Raw entries for Research Mode participants are copied into
     study.Study_Entry (separate encryption key from production).
   - The app reaches these tables ONLY through stored procedures.
     The app's database role has EXECUTE on named procedures and is
     DENIED direct table access.
   - Every procedure that returns entry text writes an audit row.
   - Row-level security is defense in depth: the app sets
     SESSION_CONTEXT('annotator_id') on every connection; predicates
     limit each annotator to assigned, non-withdrawn participants,
     with an active signed agreement, and hide restricted tiers.
   - Real identity linkage (production client ID, date offset) lives in
     study.Participant_Link, readable only by the coordinator role.
   - All constraint names are prefixed _study_ because constraint
     names are database-scoped.

   Safety tiers: 1 Normal, 2 Heated, 3 Ambiguous Monitor,
                 4 Crisis, 5 Extreme Abuse (4-5 are always restricted)

   Always Encrypted: study.Study_Entry.Entry_Text and
   study.Annotation.Notes are plain in this script so it can be tested
   on a dev copy from SSMS. For production, create the column master
   key (Key Vault) and the column encryption key CEK_study first, then
   swap in the ENCRYPTED WITH lines marked PRODUCTION below before
   running. Create the columns encrypted from the start: the security
   policy is schema-bound to these tables, which blocks the SSMS
   encryption wizard from rebuilding them later.
   ===================================================================== */

SET XACT_ABORT ON;
GO

IF SCHEMA_ID(N'study') IS NULL EXEC (N'CREATE SCHEMA study AUTHORIZATION dbo');
GO
IF SCHEMA_ID(N'study_sec') IS NULL EXEC (N'CREATE SCHEMA study_sec AUTHORIZATION dbo');
GO

/* ---------------------------------------------------------------------
   Roles
   study_app          the UCANRR API's managed identity (annotation +
                      ingestion procedures only)
   study_coordinator  the study coordinator's Entra user (enrollment,
                      agreements, assignments, withdrawal, monitoring)
   --------------------------------------------------------------------- */
IF DATABASE_PRINCIPAL_ID(N'study_app') IS NULL CREATE ROLE study_app;
IF DATABASE_PRINCIPAL_ID(N'study_coordinator') IS NULL CREATE ROLE study_coordinator;
GO

/* =====================================================================
   1. Tables
   ===================================================================== */

/* Signed privacy-preserving agreements. Stores a hash of the signed
   PDF, not the PDF itself. Keep the PDF in a private blob container
   or your records system. */
CREATE TABLE study.Privacy_Agreement (
    Agreement_ID          INT IDENTITY(1,1) CONSTRAINT PK_study_Privacy_Agreement PRIMARY KEY,
    Agreement_Version     NVARCHAR(20)  NOT NULL,
    Signer_Name           NVARCHAR(200) NOT NULL,
    Signed_At             DATETIME2(0)  NOT NULL,
    Countersigned_By      NVARCHAR(200) NOT NULL,
    Countersigned_At      DATETIME2(0)  NOT NULL,
    Signed_File_SHA256    BINARY(32)    NOT NULL,
    Signed_File_Location  NVARCHAR(400) NOT NULL,
    Expires_At            DATETIME2(0)  NULL,
    Revoked_At            DATETIME2(0)  NULL,
    Revoked_Reason        NVARCHAR(400) NULL,
    Created_At            DATETIME2(0)  NOT NULL CONSTRAINT DF_study_PA_Created DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_study_PA_Countersign CHECK (Countersigned_At >= Signed_At)
);
GO

/* People who annotate: the treating therapist, a second labeler,
   and an adjudicator. Source_User_ID is the professional's
   production user ID (string so it fits int or Firebase-style IDs). */
CREATE TABLE study.Annotator (
    Annotator_ID          INT IDENTITY(1,1) CONSTRAINT PK_study_Annotator PRIMARY KEY,
    Source_User_ID        NVARCHAR(128) NOT NULL CONSTRAINT UQ_study_Annotator_Source UNIQUE,
    Annotator_Type        VARCHAR(20)   NOT NULL
        CONSTRAINT CK_study_Annotator_Type CHECK (Annotator_Type IN ('TREATING_THERAPIST','SECOND_LABELER','ADJUDICATOR')),
    Agreement_ID          INT           NOT NULL
        CONSTRAINT FK_study_Annotator_Agreement REFERENCES study.Privacy_Agreement (Agreement_ID),
    Can_View_Restricted   BIT           NOT NULL CONSTRAINT DF_study_Annotator_Restricted DEFAULT (0),
    Is_Active             BIT           NOT NULL CONSTRAINT DF_study_Annotator_Active DEFAULT (1),
    Created_At            DATETIME2(0)  NOT NULL CONSTRAINT DF_study_Annotator_Created DEFAULT SYSUTCDATETIME(),
    Deactivated_At        DATETIME2(0)  NULL
);
GO

/* Research participants. No identifiers here. */
CREATE TABLE study.Study_Participant (
    Study_Participant_ID    INT IDENTITY(1,1) CONSTRAINT PK_study_Study_Participant PRIMARY KEY,
    Consent_Version         NVARCHAR(20)  NOT NULL,
    Research_Consent_At     DATETIME2(0)  NOT NULL,
    Screening_Instrument    NVARCHAR(50)  NOT NULL,
    Screening_Passed_At     DATETIME2(0)  NOT NULL,
    Partner_Consent_Status  VARCHAR(20)   NOT NULL
        CONSTRAINT CK_study_SP_Partner CHECK (Partner_Consent_Status IN ('BOTH_CONSENTED','SINGLE')),
    Research_Paused_At      DATETIME2(0)  NULL,
    Pause_Reason            VARCHAR(30)   NULL
        CONSTRAINT CK_study_SP_PauseReason CHECK (Pause_Reason IN ('SAFETY_ESCALATION','PARTICIPANT_REQUEST','OTHER')),
    Withdrawn_At            DATETIME2(0)  NULL,
    Enrolled_By             SYSNAME       NOT NULL CONSTRAINT DF_study_SP_EnrolledBy DEFAULT USER_NAME(),
    Created_At              DATETIME2(0)  NOT NULL CONSTRAINT DF_study_SP_Created DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_study_SP_Screening CHECK (Screening_Passed_At <= Research_Consent_At),
    CONSTRAINT CK_study_SP_PausePair CHECK ((Research_Paused_At IS NULL AND Pause_Reason IS NULL)
                                         OR (Research_Paused_At IS NOT NULL AND Pause_Reason IS NOT NULL))
);
GO

/* Linkage: production client ID and the export date offset.
   Coordinator-only. Delete at study close. */
CREATE TABLE study.Participant_Link (
    Study_Participant_ID  INT           NOT NULL CONSTRAINT PK_study_Participant_Link PRIMARY KEY
        CONSTRAINT FK_study_PL_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
    Source_Client_ID      NVARCHAR(128) NOT NULL CONSTRAINT UQ_study_PL_Source UNIQUE,
    Date_Offset_Days      SMALLINT      NOT NULL
        CONSTRAINT CK_study_PL_Offset CHECK (ABS(Date_Offset_Days) BETWEEN 30 AND 180),
    Created_At            DATETIME2(0)  NOT NULL CONSTRAINT DF_study_PL_Created DEFAULT SYSUTCDATETIME()
);
GO

/* Which annotator may see which participant. */
CREATE TABLE study.Annotator_Assignment (
    Assignment_ID         INT IDENTITY(1,1) CONSTRAINT PK_study_Annotator_Assignment PRIMARY KEY,
    Annotator_ID          INT NOT NULL CONSTRAINT FK_study_AA_Annotator REFERENCES study.Annotator (Annotator_ID),
    Study_Participant_ID  INT NOT NULL CONSTRAINT FK_study_AA_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
    Assigned_At           DATETIME2(0) NOT NULL CONSTRAINT DF_study_AA_Assigned DEFAULT SYSUTCDATETIME(),
    Revoked_At            DATETIME2(0) NULL
);
GO
CREATE UNIQUE INDEX UX_study_AA_Active
    ON study.Annotator_Assignment (Annotator_ID, Study_Participant_ID)
    WHERE Revoked_At IS NULL;
CREATE INDEX IX_study_AA_Participant
    ON study.Annotator_Assignment (Study_Participant_ID) INCLUDE (Annotator_ID, Revoked_At);
GO

/* Raw entry copies for Research Mode participants.
   Entry_Text: encrypt with Always Encrypted (randomized) before real data. */
CREATE TABLE study.Study_Entry (
    Study_Entry_ID          BIGINT IDENTITY(1,1) CONSTRAINT PK_study_Study_Entry PRIMARY KEY,
    Study_Participant_ID    INT           NOT NULL
        CONSTRAINT FK_study_SE_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
    Source_Entry_ID         NVARCHAR(128) NOT NULL CONSTRAINT UQ_study_SE_Source UNIQUE,
    Entry_Created_At        DATETIME2(0)  NOT NULL,
    Entry_Text              NVARCHAR(MAX) NOT NULL,
    /* PRODUCTION: replace the line above with
       Entry_Text NVARCHAR(MAX) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = CEK_study,
           ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL, */
    Safety_Tier_At_Capture  TINYINT       NOT NULL
        CONSTRAINT CK_study_SE_Tier CHECK (Safety_Tier_At_Capture BETWEEN 1 AND 5),
    Is_Restricted           BIT           NOT NULL CONSTRAINT DF_study_SE_Restricted DEFAULT (0),
    Ingested_At             DATETIME2(0)  NOT NULL CONSTRAINT DF_study_SE_Ingested DEFAULT SYSUTCDATETIME(),
    /* Tiers 4-5 must be restricted; lower tiers may be restricted by the coordinator. */
    CONSTRAINT CK_study_SE_RestrictTier CHECK (Safety_Tier_At_Capture < 4 OR Is_Restricted = 1)
);
GO
CREATE INDEX IX_study_SE_Participant
    ON study.Study_Entry (Study_Participant_ID, Entry_Created_At) INCLUDE (Is_Restricted, Safety_Tier_At_Capture);
GO

/* Annotations. Insert-only versioning: each save adds a row and
   clears Is_Current on the annotator's previous row for that entry.
   Notes: encrypt with Always Encrypted (randomized); free text can
   contain identifiers. */
CREATE TABLE study.Annotation (
    Annotation_ID    BIGINT IDENTITY(1,1) CONSTRAINT PK_study_Annotation PRIMARY KEY,
    Study_Entry_ID   BIGINT  NOT NULL CONSTRAINT FK_study_Ann_Entry REFERENCES study.Study_Entry (Study_Entry_ID),
    Annotator_ID     INT     NOT NULL CONSTRAINT FK_study_Ann_Annotator REFERENCES study.Annotator (Annotator_ID),
    Version_No       INT     NOT NULL,
    Is_Current       BIT     NOT NULL CONSTRAINT DF_study_Ann_Current DEFAULT (1),
    IsNeglect        BIT     NOT NULL,
    IsRepair         BIT     NOT NULL,
    IsBid            BIT     NOT NULL,
    IsCare           BIT     NOT NULL,
    IsShared         BIT     NOT NULL,
    Sentiment        SMALLINT NOT NULL CONSTRAINT CK_study_Ann_Sentiment CHECK (Sentiment BETWEEN -10 AND 10),
    Safety_Tier      TINYINT NOT NULL CONSTRAINT CK_study_Ann_Tier CHECK (Safety_Tier BETWEEN 1 AND 5),
    Notes            NVARCHAR(2000) NULL,
    /* PRODUCTION: replace the line above with
       Notes NVARCHAR(2000) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = CEK_study,
           ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL, */
    Created_At       DATETIME2(0) NOT NULL CONSTRAINT DF_study_Ann_Created DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_study_Ann_Version UNIQUE (Study_Entry_ID, Annotator_ID, Version_No)
);
GO
CREATE UNIQUE INDEX UX_study_Ann_Current
    ON study.Annotation (Study_Entry_ID, Annotator_ID)
    WHERE Is_Current = 1;
GO

/* Final adjudicated labels (one per entry). */
CREATE TABLE study.Gold_Label (
    Study_Entry_ID   BIGINT  NOT NULL CONSTRAINT PK_study_Gold_Label PRIMARY KEY
        CONSTRAINT FK_study_GL_Entry REFERENCES study.Study_Entry (Study_Entry_ID),
    Adjudicator_ID   INT     NOT NULL CONSTRAINT FK_study_GL_Adjudicator REFERENCES study.Annotator (Annotator_ID),
    Method           VARCHAR(20) NOT NULL CONSTRAINT CK_study_GL_Method CHECK (Method IN ('AGREEMENT','ADJUDICATED')),
    IsNeglect        BIT     NOT NULL,
    IsRepair         BIT     NOT NULL,
    IsBid            BIT     NOT NULL,
    IsCare           BIT     NOT NULL,
    IsShared         BIT     NOT NULL,
    Sentiment        SMALLINT NOT NULL CONSTRAINT CK_study_GL_Sentiment CHECK (Sentiment BETWEEN -10 AND 10),
    Safety_Tier      TINYINT NOT NULL CONSTRAINT CK_study_GL_Tier CHECK (Safety_Tier BETWEEN 1 AND 5),
    Created_At       DATETIME2(0) NOT NULL CONSTRAINT DF_study_GL_Created DEFAULT SYSUTCDATETIME(),
    Updated_At       DATETIME2(0) NULL
);
GO

/* Append-only access log. No foreign keys, so rows survive withdrawal.
   Holds IDs only, never text. */
CREATE TABLE study.Access_Audit (
    Audit_ID              BIGINT IDENTITY(1,1) CONSTRAINT PK_study_Access_Audit PRIMARY KEY,
    Event_At              DATETIME2(3) NOT NULL CONSTRAINT DF_study_Audit_At DEFAULT SYSUTCDATETIME(),
    Db_User               SYSNAME      NOT NULL CONSTRAINT DF_study_Audit_User DEFAULT USER_NAME(),
    Annotator_ID          INT          NULL,
    Action                VARCHAR(30)  NOT NULL CONSTRAINT CK_study_Audit_Action CHECK (Action IN (
                              'SESSION_START','SESSION_DENIED','VIEW_QUEUE','VIEW_ENTRY','VIEW_ENTRY_DENIED',
                              'SAVE_ANNOTATION','SAVE_DENIED','SAVE_GOLD','INGEST','INGEST_SKIPPED',
                              'ENROLL','ASSIGN','REVOKE_ASSIGNMENT','REGISTER_AGREEMENT','REVOKE_AGREEMENT',
                              'REGISTER_ANNOTATOR','PAUSE','RESUME','WITHDRAW')),
    Study_Participant_ID  INT          NULL,
    Study_Entry_ID        BIGINT       NULL,
    Detail                NVARCHAR(400) NULL
);
GO
CREATE INDEX IX_study_Audit_Annotator ON study.Access_Audit (Annotator_ID, Event_At);
GO
CREATE TRIGGER study.trg_Access_Audit_AppendOnly
ON study.Access_Audit
INSTEAD OF UPDATE, DELETE
AS
BEGIN
    THROW 51000, N'study.Access_Audit is append-only.', 1;
END;
GO

/* =====================================================================
   2. Row-level security
   ===================================================================== */

/* Entry visibility: coordinator, or an active annotator with a live
   agreement and an active assignment to a non-withdrawn participant.
   Restricted entries need Can_View_Restricted. */
CREATE FUNCTION study_sec.fn_entry_access (@Study_Participant_ID INT, @Is_Restricted BIT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS ok
    WHERE IS_MEMBER(N'study_coordinator') = 1
       OR EXISTS (
            SELECT 1
            FROM study.Annotator_Assignment aa
            JOIN study.Annotator a          ON a.Annotator_ID = aa.Annotator_ID
            JOIN study.Privacy_Agreement pa ON pa.Agreement_ID = a.Agreement_ID
            JOIN study.Study_Participant sp ON sp.Study_Participant_ID = aa.Study_Participant_ID
            WHERE aa.Study_Participant_ID = @Study_Participant_ID
              AND aa.Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT)
              AND aa.Revoked_At IS NULL
              AND a.Is_Active = 1
              AND pa.Revoked_At IS NULL
              AND (pa.Expires_At IS NULL OR pa.Expires_At > SYSUTCDATETIME())
              AND sp.Withdrawn_At IS NULL
              AND (@Is_Restricted = 0 OR a.Can_View_Restricted = 1));
GO

/* Annotation visibility: blind labeling. Annotators see only their
   own rows; an adjudicator sees every annotation on entries it can
   see; the coordinator sees all. */
CREATE FUNCTION study_sec.fn_annotation_access (@Annotator_ID INT, @Study_Entry_ID BIGINT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS ok
    WHERE IS_MEMBER(N'study_coordinator') = 1
       OR @Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT)
       OR EXISTS (
            SELECT 1
            FROM study.Annotator a
            JOIN study.Study_Entry e ON e.Study_Entry_ID = @Study_Entry_ID   -- Study_Entry's own filter applies
            WHERE a.Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT)
              AND a.Annotator_Type = 'ADJUDICATOR'
              AND a.Is_Active = 1);
GO

/* Annotation writes: only as yourself (coordinator exempt for withdrawal). */
CREATE FUNCTION study_sec.fn_annotation_write (@Annotator_ID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS ok
    WHERE IS_MEMBER(N'study_coordinator') = 1
       OR @Annotator_ID = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
GO

CREATE SECURITY POLICY study_sec.pol_study_access
    ADD FILTER PREDICATE study_sec.fn_entry_access (Study_Participant_ID, Is_Restricted) ON study.Study_Entry,
    ADD FILTER PREDICATE study_sec.fn_annotation_access (Annotator_ID, Study_Entry_ID) ON study.Annotation,
    ADD BLOCK  PREDICATE study_sec.fn_annotation_write (Annotator_ID) ON study.Annotation AFTER INSERT,
    ADD BLOCK  PREDICATE study_sec.fn_annotation_write (Annotator_ID) ON study.Annotation AFTER UPDATE
WITH (STATE = ON, SCHEMABINDING = ON);
GO

/* =====================================================================
   3. Procedures used by the app (study_app)
   The app must, on EVERY connection it opens or takes from the pool:
     1. EXEC study.usp_Resolve_Annotator @Source_User_ID = <signed-in user>
     2. EXEC sp_set_session_context N'annotator_id', <returned id>, @read_only = 1
   ===================================================================== */

CREATE PROCEDURE study.usp_Resolve_Annotator
    @Source_User_ID NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Annotator_ID INT;

    SELECT @Annotator_ID = a.Annotator_ID
    FROM study.Annotator a
    JOIN study.Privacy_Agreement pa ON pa.Agreement_ID = a.Agreement_ID
    WHERE a.Source_User_ID = @Source_User_ID
      AND a.Is_Active = 1
      AND pa.Revoked_At IS NULL
      AND (pa.Expires_At IS NULL OR pa.Expires_At > SYSUTCDATETIME());

    IF @Annotator_ID IS NULL
    BEGIN
        INSERT study.Access_Audit (Action, Detail) VALUES ('SESSION_DENIED', N'No active annotator/agreement for user');
        SELECT CAST(NULL AS INT) AS Annotator_ID;
        RETURN;
    END;

    INSERT study.Access_Audit (Annotator_ID, Action) VALUES (@Annotator_ID, 'SESSION_START');
    SELECT @Annotator_ID AS Annotator_ID;
END;
GO

/* Called by the Database API after the Safety API has scored the
   entry and any escalation has fired. Returns a status string. */
CREATE PROCEDURE study.usp_Ingest_Entry
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
        WHEN @PID IS NULL                     THEN 'NOT_ENROLLED'
        WHEN @Withdrawn IS NOT NULL           THEN 'WITHDRAWN'
        WHEN @Paused IS NOT NULL              THEN 'PAUSED'
        WHEN @Entry_Created_At < @Consent     THEN 'BEFORE_CONSENT'
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

CREATE PROCEDURE study.usp_Get_Annotation_Queue
    @Include_Done BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    INSERT study.Access_Audit (Annotator_ID, Action) VALUES (@Me, 'VIEW_QUEUE');

    SELECT e.Study_Entry_ID,
           e.Study_Participant_ID,
           e.Entry_Created_At,
           e.Safety_Tier_At_Capture,
           CAST(CASE WHEN a.Annotation_ID IS NULL THEN 0 ELSE 1 END AS BIT) AS Has_My_Annotation
    FROM study.Study_Entry e                      -- RLS limits rows
    LEFT JOIN study.Annotation a
           ON a.Study_Entry_ID = e.Study_Entry_ID
          AND a.Annotator_ID = @Me
          AND a.Is_Current = 1
    WHERE @Include_Done = 1 OR a.Annotation_ID IS NULL
    ORDER BY Has_My_Annotation, e.Entry_Created_At, e.Study_Entry_ID;
END;
GO

CREATE PROCEDURE study.usp_Get_Entry
    @Study_Entry_ID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    DECLARE @PID INT;
    SELECT @PID = Study_Participant_ID FROM study.Study_Entry WHERE Study_Entry_ID = @Study_Entry_ID;

    IF @PID IS NULL
    BEGIN
        -- Either it doesn't exist or RLS hides it. Both are worth reviewing.
        INSERT study.Access_Audit (Annotator_ID, Action, Study_Entry_ID)
        VALUES (@Me, 'VIEW_ENTRY_DENIED', @Study_Entry_ID);
        RETURN;  -- empty result
    END;

    INSERT study.Access_Audit (Annotator_ID, Action, Study_Participant_ID, Study_Entry_ID)
    VALUES (@Me, 'VIEW_ENTRY', @PID, @Study_Entry_ID);

    SELECT Study_Entry_ID, Study_Participant_ID, Entry_Created_At, Entry_Text, Safety_Tier_At_Capture
    FROM study.Study_Entry
    WHERE Study_Entry_ID = @Study_Entry_ID;
END;
GO

/* Annotations visible to the caller for one entry: their own, or all
   of them for an adjudicator. */
CREATE PROCEDURE study.usp_Get_Entry_Annotations
    @Study_Entry_ID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Study_Entry WHERE Study_Entry_ID = @Study_Entry_ID)
        RETURN;

    SELECT a.Annotation_ID, a.Annotator_ID, an.Annotator_Type, a.Version_No,
           a.IsNeglect, a.IsRepair, a.IsBid, a.IsCare, a.IsShared,
           a.Sentiment, a.Safety_Tier, a.Notes, a.Created_At
    FROM study.Annotation a
    JOIN study.Annotator an ON an.Annotator_ID = a.Annotator_ID
    WHERE a.Study_Entry_ID = @Study_Entry_ID
      AND a.Is_Current = 1
    ORDER BY a.Annotator_ID;
END;
GO

CREATE PROCEDURE study.usp_Save_Annotation
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

CREATE PROCEDURE study.usp_Save_Gold_Label
    @Study_Entry_ID BIGINT,
    @Method VARCHAR(20),
    @IsNeglect BIT, @IsRepair BIT, @IsBid BIT, @IsCare BIT, @IsShared BIT,
    @Sentiment SMALLINT,
    @Safety_Tier TINYINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);

    IF NOT EXISTS (SELECT 1 FROM study.Annotator
                   WHERE Annotator_ID = @Me AND Annotator_Type = 'ADJUDICATOR' AND Is_Active = 1)
        THROW 51005, N'Only an active adjudicator can save gold labels.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Study_Entry WHERE Study_Entry_ID = @Study_Entry_ID)
        THROW 51002, N'Entry not found or not accessible.', 1;

    IF (SELECT COUNT(*) FROM study.Annotation
        WHERE Study_Entry_ID = @Study_Entry_ID AND Is_Current = 1 AND Annotator_ID <> @Me) < 2
        THROW 51006, N'Gold label needs two independent annotations first.', 1;

    MERGE study.Gold_Label AS t
    USING (SELECT @Study_Entry_ID AS Study_Entry_ID) AS s
       ON t.Study_Entry_ID = s.Study_Entry_ID
    WHEN MATCHED THEN UPDATE SET
        Adjudicator_ID = @Me, Method = @Method,
        IsNeglect = @IsNeglect, IsRepair = @IsRepair, IsBid = @IsBid, IsCare = @IsCare, IsShared = @IsShared,
        Sentiment = @Sentiment, Safety_Tier = @Safety_Tier, Updated_At = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT
        (Study_Entry_ID, Adjudicator_ID, Method, IsNeglect, IsRepair, IsBid, IsCare, IsShared, Sentiment, Safety_Tier)
        VALUES (@Study_Entry_ID, @Me, @Method, @IsNeglect, @IsRepair, @IsBid, @IsCare, @IsShared, @Sentiment, @Safety_Tier);

    INSERT study.Access_Audit (Annotator_ID, Action, Study_Entry_ID, Detail)
    VALUES (@Me, 'SAVE_GOLD', @Study_Entry_ID, @Method);
END;
GO

/* =====================================================================
   4. Coordinator procedures (study_coordinator)
   ===================================================================== */

CREATE PROCEDURE study.usp_Register_Agreement
    @Agreement_Version NVARCHAR(20),
    @Signer_Name NVARCHAR(200),
    @Signed_At DATETIME2(0),
    @Countersigned_By NVARCHAR(200),
    @Countersigned_At DATETIME2(0),
    @Signed_File_SHA256 BINARY(32),
    @Signed_File_Location NVARCHAR(400),
    @Expires_At DATETIME2(0) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT study.Privacy_Agreement (Agreement_Version, Signer_Name, Signed_At, Countersigned_By,
                                    Countersigned_At, Signed_File_SHA256, Signed_File_Location, Expires_At)
    VALUES (@Agreement_Version, @Signer_Name, @Signed_At, @Countersigned_By,
            @Countersigned_At, @Signed_File_SHA256, @Signed_File_Location, @Expires_At);

    DECLARE @ID INT = SCOPE_IDENTITY();
    INSERT study.Access_Audit (Action, Detail) VALUES ('REGISTER_AGREEMENT', CONCAT(N'agreement ', @ID));
    SELECT @ID AS Agreement_ID;
END;
GO

CREATE PROCEDURE study.usp_Revoke_Agreement
    @Agreement_ID INT,
    @Reason NVARCHAR(400)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE study.Privacy_Agreement
       SET Revoked_At = SYSUTCDATETIME(), Revoked_Reason = @Reason
     WHERE Agreement_ID = @Agreement_ID AND Revoked_At IS NULL;
    INSERT study.Access_Audit (Action, Detail) VALUES ('REVOKE_AGREEMENT', CONCAT(N'agreement ', @Agreement_ID));
END;
GO

CREATE PROCEDURE study.usp_Register_Annotator
    @Source_User_ID NVARCHAR(128),
    @Annotator_Type VARCHAR(20),
    @Agreement_ID INT,
    @Can_View_Restricted BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    INSERT study.Annotator (Source_User_ID, Annotator_Type, Agreement_ID, Can_View_Restricted)
    VALUES (@Source_User_ID, @Annotator_Type, @Agreement_ID, @Can_View_Restricted);

    DECLARE @ID INT = SCOPE_IDENTITY();
    INSERT study.Access_Audit (Annotator_ID, Action, Detail) VALUES (@ID, 'REGISTER_ANNOTATOR', @Annotator_Type);
    SELECT @ID AS Annotator_ID;
END;
GO

CREATE PROCEDURE study.usp_Enroll_Participant
    @Source_Client_ID NVARCHAR(128),
    @Consent_Version NVARCHAR(20),
    @Research_Consent_At DATETIME2(0),
    @Screening_Instrument NVARCHAR(50),
    @Screening_Passed_At DATETIME2(0),
    @Partner_Consent_Status VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @Screening_Passed_At > @Research_Consent_At
        THROW 51007, N'Screening must be passed before research consent is recorded.', 1;

    -- Date offset from a CSPRNG: magnitude 30..180 days, random sign, never 0.
    DECLARE @Mag INT = 30 + (CAST(CRYPT_GEN_RANDOM(2) AS INT) % 151);
    DECLARE @Offset SMALLINT = CASE WHEN CAST(CRYPT_GEN_RANDOM(1) AS INT) % 2 = 0 THEN -@Mag ELSE @Mag END;

    BEGIN TRAN;
        INSERT study.Study_Participant (Consent_Version, Research_Consent_At, Screening_Instrument,
                                        Screening_Passed_At, Partner_Consent_Status)
        VALUES (@Consent_Version, @Research_Consent_At, @Screening_Instrument,
                @Screening_Passed_At, @Partner_Consent_Status);

        DECLARE @PID INT = SCOPE_IDENTITY();

        INSERT study.Participant_Link (Study_Participant_ID, Source_Client_ID, Date_Offset_Days)
        VALUES (@PID, @Source_Client_ID, @Offset);

        INSERT study.Access_Audit (Action, Study_Participant_ID) VALUES ('ENROLL', @PID);
    COMMIT;

    SELECT @PID AS Study_Participant_ID;
END;
GO

CREATE PROCEDURE study.usp_Assign_Annotator
    @Annotator_ID INT,
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT study.Annotator_Assignment (Annotator_ID, Study_Participant_ID)
    VALUES (@Annotator_ID, @Study_Participant_ID);
    INSERT study.Access_Audit (Annotator_ID, Action, Study_Participant_ID)
    VALUES (@Annotator_ID, 'ASSIGN', @Study_Participant_ID);
END;
GO

CREATE PROCEDURE study.usp_Revoke_Assignment
    @Annotator_ID INT,
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE study.Annotator_Assignment
       SET Revoked_At = SYSUTCDATETIME()
     WHERE Annotator_ID = @Annotator_ID AND Study_Participant_ID = @Study_Participant_ID AND Revoked_At IS NULL;
    INSERT study.Access_Audit (Annotator_ID, Action, Study_Participant_ID)
    VALUES (@Annotator_ID, 'REVOKE_ASSIGNMENT', @Study_Participant_ID);
END;
GO

/* Mid-study safety escalation or participant request: stop ingesting
   new entries. Existing entries stay as they are until the protocol
   decision (retain / exclude / restrict) is applied. */
CREATE PROCEDURE study.usp_Pause_Participant
    @Study_Participant_ID INT,
    @Reason VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE study.Study_Participant
       SET Research_Paused_At = SYSUTCDATETIME(), Pause_Reason = @Reason
     WHERE Study_Participant_ID = @Study_Participant_ID AND Research_Paused_At IS NULL;
    INSERT study.Access_Audit (Action, Study_Participant_ID, Detail) VALUES ('PAUSE', @Study_Participant_ID, @Reason);
END;
GO

CREATE PROCEDURE study.usp_Resume_Participant
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE study.Study_Participant
       SET Research_Paused_At = NULL, Pause_Reason = NULL
     WHERE Study_Participant_ID = @Study_Participant_ID;
    INSERT study.Access_Audit (Action, Study_Participant_ID) VALUES ('RESUME', @Study_Participant_ID);
END;
GO

/* Restrict one entry by hand (e.g., a reviewer judges a tier-3 entry
   too sensitive). */
CREATE PROCEDURE study.usp_Restrict_Entry
    @Study_Entry_ID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE study.Study_Entry SET Is_Restricted = 1 WHERE Study_Entry_ID = @Study_Entry_ID;
END;
GO

/* Withdrawal: removes the participant's entries, annotations and gold
   labels, revokes assignments, and (optionally) deletes the linkage
   row. Audit rows are kept; they hold IDs only. */
CREATE PROCEDURE study.usp_Withdraw_Participant
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

/* =====================================================================
   5. Monitoring views (coordinator)
   ===================================================================== */

CREATE VIEW study.vw_Annotation_Progress
AS
SELECT e.Study_Participant_ID,
       COUNT(*)                                            AS Entries,
       SUM(CAST(e.Is_Restricted AS INT))                   AS Restricted_Entries,
       SUM(CASE WHEN x.N >= 1 THEN 1 ELSE 0 END)           AS Labeled_Once,
       SUM(CASE WHEN x.N >= 2 THEN 1 ELSE 0 END)           AS Labeled_Twice,
       SUM(CASE WHEN g.Study_Entry_ID IS NULL THEN 0 ELSE 1 END) AS Gold_Final
FROM study.Study_Entry e
OUTER APPLY (SELECT COUNT(*) AS N FROM study.Annotation a
             WHERE a.Study_Entry_ID = e.Study_Entry_ID AND a.Is_Current = 1) x
LEFT JOIN study.Gold_Label g ON g.Study_Entry_ID = e.Study_Entry_ID
GROUP BY e.Study_Participant_ID;
GO

/* Weekly audit review: activity per annotator per day, with denied
   events broken out. */
CREATE VIEW study.vw_Audit_Daily
AS
SELECT CAST(Event_At AS DATE) AS Event_Date,
       Annotator_ID,
       Db_User,
       SUM(CASE WHEN Action = 'VIEW_ENTRY' THEN 1 ELSE 0 END)                         AS Entries_Viewed,
       SUM(CASE WHEN Action = 'SAVE_ANNOTATION' THEN 1 ELSE 0 END)                    AS Annotations_Saved,
       SUM(CASE WHEN Action IN ('VIEW_ENTRY_DENIED','SAVE_DENIED','SESSION_DENIED') THEN 1 ELSE 0 END) AS Denied_Events,
       COUNT(DISTINCT Study_Participant_ID)                                           AS Participants_Touched
FROM study.Access_Audit
GROUP BY CAST(Event_At AS DATE), Annotator_ID, Db_User;
GO

/* =====================================================================
   6. Permissions
   Procedures are owned by dbo, so ownership chaining lets them reach
   the tables even though direct access is denied.
   ===================================================================== */

DENY SELECT, INSERT, UPDATE, DELETE ON SCHEMA::study TO study_app;
DENY SELECT, INSERT, UPDATE, DELETE ON SCHEMA::study_sec TO study_app;

GRANT EXECUTE ON study.usp_Resolve_Annotator      TO study_app;
GRANT EXECUTE ON study.usp_Ingest_Entry           TO study_app;
GRANT EXECUTE ON study.usp_Get_Annotation_Queue   TO study_app;
GRANT EXECUTE ON study.usp_Get_Entry              TO study_app;
GRANT EXECUTE ON study.usp_Get_Entry_Annotations  TO study_app;
GRANT EXECUTE ON study.usp_Save_Annotation        TO study_app;
GRANT EXECUTE ON study.usp_Save_Gold_Label        TO study_app;

GRANT EXECUTE ON study.usp_Register_Agreement     TO study_coordinator;
GRANT EXECUTE ON study.usp_Revoke_Agreement       TO study_coordinator;
GRANT EXECUTE ON study.usp_Register_Annotator     TO study_coordinator;
GRANT EXECUTE ON study.usp_Enroll_Participant     TO study_coordinator;
GRANT EXECUTE ON study.usp_Assign_Annotator       TO study_coordinator;
GRANT EXECUTE ON study.usp_Revoke_Assignment      TO study_coordinator;
GRANT EXECUTE ON study.usp_Pause_Participant      TO study_coordinator;
GRANT EXECUTE ON study.usp_Resume_Participant     TO study_coordinator;
GRANT EXECUTE ON study.usp_Restrict_Entry         TO study_coordinator;
GRANT EXECUTE ON study.usp_Withdraw_Participant   TO study_coordinator;
-- Coordinator can read everything for monitoring. Once Always Encrypted
-- is on, Entry_Text and Notes are ciphertext to the coordinator unless
-- they are also granted access to the column master key in Key Vault.
GRANT SELECT ON SCHEMA::study TO study_coordinator;
DENY  UPDATE, DELETE ON study.Access_Audit TO study_coordinator;
GO

/* Map identities to roles (run once per environment; edit names):
   CREATE USER [ucanrr-api-appservice] FROM EXTERNAL PROVIDER;   -- managed identity name
   ALTER ROLE study_app ADD MEMBER [ucanrr-api-appservice];
   CREATE USER [coordinator@yourdomain.com] FROM EXTERNAL PROVIDER;
   ALTER ROLE study_coordinator ADD MEMBER [coordinator@yourdomain.com];
*/
