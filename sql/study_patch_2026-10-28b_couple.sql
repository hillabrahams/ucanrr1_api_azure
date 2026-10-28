/* =====================================================================
   UCANRR study schema: patch 2026-10-28b, couple-level enrollment and
   clinician attestation (conflict rows 4 and 5)
   Run as the server admin, in the study database, AFTER
   study_patch_2026-10-28_five_tier.sql. Safe to re-run.

   Row 4 (§4.3, B2 §1): both partners consent or the couple does not
   enroll; one withdrawal ends both. Until now each partner was enrolled
   on their own, possibly with only that partner consenting.
     1. study.Study_Participant.Couple_ID: both partners of a couple
        share it. It is kept after the link to the client is deleted.
     2. usp_Enroll_Couple enrolls both partners at once, each with their
        own consent time; usp_Enroll_Participant is removed. Both
        partners get the same date offset, so their timelines line up.
     3. Withdrawing, pausing or resuming either partner applies to both.
        Withdrawal deletes both partners' study data.
     4. Entries count only from the time the couple enrolled: the later
        of the two partners' consents.
     Each partner's B3 (text retention) choice stays their own.

   Row 5 (§4.4, B4 §3): eligibility is the clinician's judgment; no
   instrument, cut-off or score is kept.
     5. Clinician_Attestation_Date replaces Screening_Instrument and
        Screening_Passed_At. Partner_Consent_Status is removed (it is
        always both partners now).
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. New columns, filled for any existing participants.
   --------------------------------------------------------------------- */
IF COL_LENGTH(N'study.Study_Participant', N'Couple_ID') IS NULL
    ALTER TABLE study.Study_Participant ADD Couple_ID INT NULL;
GO
IF COL_LENGTH(N'study.Study_Participant', N'Clinician_Attestation_Date') IS NULL
    ALTER TABLE study.Study_Participant ADD Clinician_Attestation_Date DATE NULL;
GO

-- Existing participants: the screening date becomes the attestation date.
IF COL_LENGTH(N'study.Study_Participant', N'Screening_Passed_At') IS NOT NULL
    EXEC (N'UPDATE study.Study_Participant
               SET Clinician_Attestation_Date = CAST(Screening_Passed_At AS DATE)
             WHERE Clinician_Attestation_Date IS NULL;');
GO

-- Existing participants: partners of one client record ("<Client.Id>-<partner>")
-- form a couple; a participant whose link is gone is a couple of one.
WITH l AS (
    SELECT pl.Study_Participant_ID,
           LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID + '-') - 1) AS Client
    FROM study.Participant_Link pl
)
UPDATE sp
   SET Couple_ID = ISNULL((SELECT MIN(l2.Study_Participant_ID)
                           FROM l l1 JOIN l l2 ON l2.Client = l1.Client
                           WHERE l1.Study_Participant_ID = sp.Study_Participant_ID),
                          sp.Study_Participant_ID)
FROM study.Study_Participant sp
WHERE sp.Couple_ID IS NULL;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'study.Study_Participant')
           AND name = N'Couple_ID' AND is_nullable = 1)
    ALTER TABLE study.Study_Participant ALTER COLUMN Couple_ID INT NOT NULL;
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'study.Study_Participant')
           AND name = N'Clinician_Attestation_Date' AND is_nullable = 1)
    ALTER TABLE study.Study_Participant ALTER COLUMN Clinician_Attestation_Date DATE NOT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'study.Study_Participant')
               AND name = N'IX_study_SP_Couple')
    CREATE INDEX IX_study_SP_Couple ON study.Study_Participant (Couple_ID);
GO

-- The attestation comes before enrollment.
IF OBJECT_ID(N'study.CK_study_SP_Attestation', N'C') IS NULL
BEGIN
    IF EXISTS (SELECT 1 FROM study.Study_Participant
               WHERE Clinician_Attestation_Date > CAST(Created_At AS DATE))
        ALTER TABLE study.Study_Participant WITH NOCHECK ADD CONSTRAINT CK_study_SP_Attestation
            CHECK (Clinician_Attestation_Date <= CAST(Created_At AS DATE));
    ELSE
        ALTER TABLE study.Study_Participant ADD CONSTRAINT CK_study_SP_Attestation
            CHECK (Clinician_Attestation_Date <= CAST(Created_At AS DATE));
END;
GO

/* ---------------------------------------------------------------------
   5. Remove the screening instrument and the single-partner status.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'study.CK_study_SP_Screening', N'C') IS NOT NULL
    ALTER TABLE study.Study_Participant DROP CONSTRAINT CK_study_SP_Screening;
IF OBJECT_ID(N'study.CK_study_SP_Partner', N'C') IS NOT NULL
    ALTER TABLE study.Study_Participant DROP CONSTRAINT CK_study_SP_Partner;
GO
IF COL_LENGTH(N'study.Study_Participant', N'Screening_Instrument') IS NOT NULL
    ALTER TABLE study.Study_Participant DROP COLUMN Screening_Instrument;
IF COL_LENGTH(N'study.Study_Participant', N'Screening_Passed_At') IS NOT NULL
    ALTER TABLE study.Study_Participant DROP COLUMN Screening_Passed_At;
IF COL_LENGTH(N'study.Study_Participant', N'Partner_Consent_Status') IS NOT NULL
    ALTER TABLE study.Study_Participant DROP COLUMN Partner_Consent_Status;
GO

/* ---------------------------------------------------------------------
   2. Enroll a couple: both partners, or neither.
   --------------------------------------------------------------------- */
DROP PROCEDURE IF EXISTS study.usp_Enroll_Participant;
GO
CREATE OR ALTER PROCEDURE study.usp_Enroll_Couple
    @Client_ID INT,
    @Consent_Version NVARCHAR(20),
    @Partner1_Consent_At DATETIME2(0),
    @Partner2_Consent_At DATETIME2(0),
    @Clinician_Attestation_Date DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @Partner1_Consent_At IS NULL OR @Partner2_Consent_At IS NULL
        THROW 51030, N'Both partners must give research consent before the couple is enrolled.', 1;
    IF @Clinician_Attestation_Date IS NULL OR @Clinician_Attestation_Date > CAST(SYSUTCDATETIME() AS DATE)
        THROW 51031, N'The clinician''s attestation must be received before the couple is enrolled.', 1;
    IF EXISTS (SELECT 1 FROM study.Participant_Link
               WHERE Source_Client_ID IN (CONCAT(@Client_ID, N'-1'), CONCAT(@Client_ID, N'-2')))
        THROW 51032, N'This couple is already enrolled.', 1;

    -- Date offset from a CSPRNG: magnitude 30..180 days, random sign, never 0.
    -- One offset for the couple, so the partners' timelines line up.
    DECLARE @Mag INT = 30 + (CAST(CRYPT_GEN_RANDOM(2) AS INT) % 151);
    DECLARE @Offset SMALLINT = CASE WHEN CAST(CRYPT_GEN_RANDOM(1) AS INT) % 2 = 0 THEN -@Mag ELSE @Mag END;
    DECLARE @P1 INT, @P2 INT;

    BEGIN TRAN;
        INSERT study.Study_Participant (Couple_ID, Consent_Version, Research_Consent_At, Clinician_Attestation_Date)
        VALUES (0, @Consent_Version, @Partner1_Consent_At, @Clinician_Attestation_Date);
        SET @P1 = SCOPE_IDENTITY();
        UPDATE study.Study_Participant SET Couple_ID = @P1 WHERE Study_Participant_ID = @P1;

        INSERT study.Study_Participant (Couple_ID, Consent_Version, Research_Consent_At, Clinician_Attestation_Date)
        VALUES (@P1, @Consent_Version, @Partner2_Consent_At, @Clinician_Attestation_Date);
        SET @P2 = SCOPE_IDENTITY();

        INSERT study.Participant_Link (Study_Participant_ID, Source_Client_ID, Date_Offset_Days)
        VALUES (@P1, CONCAT(@Client_ID, N'-1'), @Offset),
               (@P2, CONCAT(@Client_ID, N'-2'), @Offset);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        VALUES ('ENROLL', @P1, CONCAT(N'couple ', @P1, N', partner 1')),
               ('ENROLL', @P2, CONCAT(N'couple ', @P1, N', partner 2'));
    COMMIT;

    SELECT @P1 AS Couple_ID, @P1 AS Partner1_Participant_ID, @P2 AS Partner2_Participant_ID;
END;
GO

/* ---------------------------------------------------------------------
   3. Withdraw, pause and resume apply to the whole couple.
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

CREATE OR ALTER PROCEDURE study.usp_Pause_Participant
    @Study_Participant_ID INT,
    @Reason VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Paused TABLE (Study_Participant_ID INT);
    BEGIN TRAN;
        UPDATE study.Study_Participant
           SET Research_Paused_At = SYSUTCDATETIME(), Pause_Reason = @Reason
        OUTPUT inserted.Study_Participant_ID INTO @Paused
         WHERE Couple_ID = (SELECT Couple_ID FROM study.Study_Participant WHERE Study_Participant_ID = @Study_Participant_ID)
           AND Research_Paused_At IS NULL AND Withdrawn_At IS NULL;
        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        SELECT 'PAUSE', Study_Participant_ID, @Reason FROM @Paused;
    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE study.usp_Resume_Participant
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Resumed TABLE (Study_Participant_ID INT);
    BEGIN TRAN;
        UPDATE study.Study_Participant
           SET Research_Paused_At = NULL, Pause_Reason = NULL
        OUTPUT inserted.Study_Participant_ID INTO @Resumed
         WHERE Couple_ID = (SELECT Couple_ID FROM study.Study_Participant WHERE Study_Participant_ID = @Study_Participant_ID)
           AND Research_Paused_At IS NOT NULL AND Withdrawn_At IS NULL;
        INSERT study.Access_Audit (Action, Study_Participant_ID)
        SELECT 'RESUME', Study_Participant_ID FROM @Resumed;
    COMMIT;
END;
GO

/* ---------------------------------------------------------------------
   4. Ingestion: entries count from the couple's enrollment (the later
      of the two consents). Otherwise as in patch 2026-10-20.
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

    SELECT @PID = sp.Study_Participant_ID,
           @Consent = (SELECT MAX(c.Research_Consent_At) FROM study.Study_Participant c WHERE c.Couple_ID = sp.Couple_ID),
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

GRANT EXECUTE ON study.usp_Enroll_Couple TO study_coordinator;
GO

/* ---------------------------------------------------------------------
   Checks: every row should read 'ok'.
   --------------------------------------------------------------------- */
SELECT 'Couple_ID and Clinician_Attestation_Date in place' AS [Check],
       CASE WHEN COL_LENGTH(N'study.Study_Participant', N'Couple_ID') IS NOT NULL
             AND COL_LENGTH(N'study.Study_Participant', N'Clinician_Attestation_Date') IS NOT NULL
            THEN 'ok' ELSE 'MISSING' END AS Result
UNION ALL
SELECT 'Screening and single-partner columns removed',
       CASE WHEN COL_LENGTH(N'study.Study_Participant', N'Screening_Instrument') IS NULL
             AND COL_LENGTH(N'study.Study_Participant', N'Screening_Passed_At') IS NULL
             AND COL_LENGTH(N'study.Study_Participant', N'Partner_Consent_Status') IS NULL
            THEN 'ok' ELSE 'STILL THERE' END
UNION ALL
SELECT 'usp_Enroll_Couple replaces usp_Enroll_Participant',
       CASE WHEN OBJECT_ID(N'study.usp_Enroll_Couple', N'P') IS NOT NULL
             AND OBJECT_ID(N'study.usp_Enroll_Participant', N'P') IS NULL
            THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'Withdrawal applies to the couple',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Withdraw_Participant')) LIKE N'%Couple_ID%'
            THEN 'ok' ELSE 'MISSING' END;
GO
