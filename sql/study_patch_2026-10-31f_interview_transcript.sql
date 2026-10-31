/* =====================================================================
   UCANRR study schema: patch 2026-10-31f, exit interview transcripts
   (protocol §9.4; Attachment G, A6 + A12)

   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-31e_assessment_response.sql. Safe to re-run.

   Scope decided for this round: transcript text only, no audio. A
   coordinator records the transcript after conducting the exit
   interview (there's no app-side capture path for this, unlike journal
   entries); it's de-identified through the same pipeline already built
   for journal text (surrogate.py, Key Vault HMAC, Azure AI Language),
   as a parallel table pair rather than folding into Study_Entry --
   Study_Entry's Safety_Tier_At_Capture / Is_Restricted columns are
   journal-specific and don't apply here, and there's no annotator-
   rating workflow for interviews.

   study.Interview_Transcript / study.Interview_Transcript_Deid mirror
   study.Study_Entry / study.Entry_Deid. Reuses the same
   Participant_Link.Date_Offset_Days for date-shifting and the same
   Surrogate_Map, so a participant's name gets the same surrogate in
   their interview as in their journal entries.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Tables.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'study.Interview_Transcript', N'U') IS NULL
    CREATE TABLE study.Interview_Transcript (
        Interview_Transcript_ID BIGINT IDENTITY(1,1) CONSTRAINT PK_study_IT PRIMARY KEY,
        Study_Participant_ID    INT           NOT NULL
            CONSTRAINT FK_study_IT_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
        Interview_Conducted_At  DATETIME2(0)  NOT NULL,
        Transcript_Text         NVARCHAR(MAX) NOT NULL,
        /* PRODUCTION: encrypt with Always Encrypted (randomized), as for
           Study_Entry.Entry_Text -- the same identifiable-free-text risk. */
        Recorded_By             SYSNAME       NOT NULL CONSTRAINT DF_study_IT_By DEFAULT USER_NAME(),
        Created_At              DATETIME2(0)  NOT NULL CONSTRAINT DF_study_IT_Created DEFAULT SYSUTCDATETIME()
    );
GO
IF OBJECT_ID(N'study.Interview_Transcript', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_study_IT_Participant'
                   AND object_id = OBJECT_ID(N'study.Interview_Transcript'))
    CREATE INDEX IX_study_IT_Participant ON study.Interview_Transcript (Study_Participant_ID);
GO

IF OBJECT_ID(N'study.Interview_Transcript_Deid', N'U') IS NULL
    CREATE TABLE study.Interview_Transcript_Deid (
        Interview_Transcript_ID BIGINT        NOT NULL CONSTRAINT PK_study_ITD PRIMARY KEY
            CONSTRAINT FK_study_ITD_Transcript REFERENCES study.Interview_Transcript (Interview_Transcript_ID),
        Deid_Text                NVARCHAR(MAX) NOT NULL,
        Pipeline_Version          NVARCHAR(40)  NOT NULL,
        Saved_At                  DATETIME2(0)  NOT NULL CONSTRAINT DF_study_ITD_Saved DEFAULT SYSUTCDATETIME()
    );
GO

/* ---------------------------------------------------------------------
   2. Recording, coordinator only.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Record_Interview_Transcript
    @Study_Participant_ID    INT,
    @Interview_Conducted_At  DATETIME2(0),
    @Transcript_Text         NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Recording an interview transcript must be run by a study coordinator.', 1;
    IF NOT EXISTS (SELECT 1 FROM study.Study_Participant
                   WHERE Study_Participant_ID = @Study_Participant_ID AND Withdrawn_At IS NULL)
        THROW 51002, N'Participant not found, or has withdrawn.', 1;
    IF LEN(LTRIM(RTRIM(ISNULL(@Transcript_Text, N'')))) = 0
        THROW 51024, N'Transcript text is empty.', 1;

    DECLARE @ID BIGINT;
    BEGIN TRAN;
        INSERT study.Interview_Transcript (Study_Participant_ID, Interview_Conducted_At, Transcript_Text)
        VALUES (@Study_Participant_ID, @Interview_Conducted_At, @Transcript_Text);
        SET @ID = SCOPE_IDENTITY();

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        VALUES ('INGEST', @Study_Participant_ID, CONCAT(N'interview ', @ID));
    COMMIT;

    SELECT @ID AS Interview_Transcript_ID;
END;
GO
GRANT EXECUTE ON study.usp_Record_Interview_Transcript TO study_coordinator;
GO

/* ---------------------------------------------------------------------
   3. De-identification, same shape as usp_Get_Entries_For_Deid /
      usp_Save_Deid_Text, same study_deid grantee.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Get_Interview_Transcripts_For_Deid
    @Max_Rows INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Out TABLE (Interview_Transcript_ID BIGINT PRIMARY KEY, Study_Participant_ID INT);

    INSERT @Out (Interview_Transcript_ID, Study_Participant_ID)
    SELECT TOP (@Max_Rows) it.Interview_Transcript_ID, it.Study_Participant_ID
    FROM study.Interview_Transcript it
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = it.Study_Participant_ID
    WHERE sp.Withdrawn_At IS NULL
      AND NOT EXISTS (SELECT 1 FROM study.Interview_Transcript_Deid d
                      WHERE d.Interview_Transcript_ID = it.Interview_Transcript_ID)
    ORDER BY it.Interview_Transcript_ID;

    INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
    SELECT 'DEID_READ', Study_Participant_ID, CONCAT(N'interview ', Interview_Transcript_ID) FROM @Out;

    SELECT it.Interview_Transcript_ID, it.Study_Participant_ID, it.Interview_Conducted_At, it.Transcript_Text,
           pl.Date_Offset_Days
    FROM @Out o
    JOIN study.Interview_Transcript it ON it.Interview_Transcript_ID = o.Interview_Transcript_ID
    LEFT JOIN study.Participant_Link pl ON pl.Study_Participant_ID = it.Study_Participant_ID
    ORDER BY it.Interview_Transcript_ID;
END;
GO

CREATE OR ALTER PROCEDURE study.usp_Save_Interview_Transcript_Deid
    @Interview_Transcript_ID BIGINT,
    @Deid_Text                NVARCHAR(MAX),
    @Pipeline_Version          NVARCHAR(40)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @PID INT, @Withdrawn DATETIME2(0);

    SELECT @PID = it.Study_Participant_ID, @Withdrawn = sp.Withdrawn_At
    FROM study.Interview_Transcript it
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = it.Study_Participant_ID
    WHERE it.Interview_Transcript_ID = @Interview_Transcript_ID;

    IF @PID IS NULL THROW 51002, N'Interview transcript not found.', 1;
    IF @Withdrawn IS NOT NULL THROW 51022, N'The participant has withdrawn.', 1;
    IF LEN(LTRIM(RTRIM(ISNULL(@Deid_Text, N'')))) = 0 THROW 51024, N'De-identified text is empty.', 1;

    BEGIN TRAN;
        MERGE study.Interview_Transcript_Deid AS t
        USING (SELECT @Interview_Transcript_ID AS Interview_Transcript_ID) AS s
           ON t.Interview_Transcript_ID = s.Interview_Transcript_ID
        WHEN MATCHED THEN UPDATE SET
            Deid_Text = @Deid_Text, Pipeline_Version = @Pipeline_Version, Saved_At = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (Interview_Transcript_ID, Deid_Text, Pipeline_Version)
            VALUES (@Interview_Transcript_ID, @Deid_Text, @Pipeline_Version);

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        VALUES ('DEID_SAVE', @PID, CONCAT(N'interview ', @Interview_Transcript_ID, N'; ', @Pipeline_Version));
    COMMIT;
END;
GO

GRANT EXECUTE ON study.usp_Get_Interview_Transcripts_For_Deid TO study_deid;
GRANT EXECUTE ON study.usp_Save_Interview_Transcript_Deid     TO study_deid;
GO

/* ---------------------------------------------------------------------
   4. The linking-key erasure rule (study_patch_2026-10-31d) only checked
      Study_Entry for pending de-identification. Interview transcripts
      need Date_Offset_Days too, so erasing the link while one is still
      pending would strand it un-de-identifiable forever.
   --------------------------------------------------------------------- */
CREATE OR ALTER FUNCTION study.fn_linkage_erasure_eligible (@Grace_Days INT)
RETURNS TABLE
AS
RETURN (
    SELECT sp.Study_Participant_ID,
           CASE WHEN sp.Withdrawn_At IS NOT NULL THEN 'WITHDRAWN' ELSE 'STUDY_CLOSED' END AS Reason
    FROM study.Study_Participant sp
    JOIN study.Participant_Link pl ON pl.Study_Participant_ID = sp.Study_Participant_ID
    WHERE sp.Withdrawn_At IS NOT NULL
       OR (
            EXISTS (SELECT 1 FROM study.Study_Close sc
                    WHERE DATEDIFF(DAY, sc.Closed_At, SYSUTCDATETIME()) >= @Grace_Days)
            AND NOT EXISTS (
                SELECT 1 FROM study.Study_Entry e
                WHERE e.Study_Participant_ID = sp.Study_Participant_ID
                  AND e.Entry_Text IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM study.Entry_Deid d WHERE d.Study_Entry_ID = e.Study_Entry_ID)
            )
            AND NOT EXISTS (
                SELECT 1 FROM study.Interview_Transcript it
                WHERE it.Study_Participant_ID = sp.Study_Participant_ID
                  AND NOT EXISTS (SELECT 1 FROM study.Interview_Transcript_Deid d
                                  WHERE d.Interview_Transcript_ID = it.Interview_Transcript_ID)
            )
          )
);
GO

/* ---------------------------------------------------------------------
   5. Withdrawal erases interview transcripts too.
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

        DELETE itd FROM study.Interview_Transcript_Deid itd
          JOIN study.Interview_Transcript it ON it.Interview_Transcript_ID = itd.Interview_Transcript_ID
         WHERE it.Study_Participant_ID IN (SELECT Study_Participant_ID FROM @Couple);

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
SELECT v.name,
       CASE WHEN OBJECT_ID('study.' + v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('Interview_Transcript'), ('Interview_Transcript_Deid'),
             ('usp_Record_Interview_Transcript'), ('usp_Get_Interview_Transcripts_For_Deid'),
             ('usp_Save_Interview_Transcript_Deid')) v(name)
UNION ALL
SELECT 'study_deid has EXECUTE on both interview de-id procedures',
       CASE WHEN (
            SELECT COUNT(DISTINCT o.name)
            FROM sys.database_permissions p
            JOIN sys.database_principals r ON r.principal_id = p.grantee_principal_id
            JOIN sys.objects o ON o.object_id = p.major_id
            WHERE r.name = N'study_deid' AND p.permission_name = 'EXECUTE'
              AND o.name IN (N'usp_Get_Interview_Transcripts_For_Deid', N'usp_Save_Interview_Transcript_Deid')
            ) = 2 THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'usp_Withdraw_Participant erases interview transcripts',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Withdraw_Participant')) LIKE N'%DELETE FROM study.Interview_Transcript%'
            THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'fn_linkage_erasure_eligible checks pending interview transcripts',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.fn_linkage_erasure_eligible', N'IF')) LIKE N'%Interview_Transcript%'
            THEN 'ok' ELSE 'MISSING' END;
GO
