/* =====================================================================
   UCANRR study schema: patch 2026-10-28, one safety tier scale
   (conflict row 3; protocol §8.1)
   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-20_text_retention.sql. Safe to re-run.

   Every stored safety tier is now on the five-tier scale:
       1 Normal, 2 Heated, 3 Ambiguous Monitor, 4 Crisis, 5 Extreme Abuse
   The safety service still answers 0–3 (frozen); the app and the main API
   convert by label from 2026-10-28.

     1. Existing dbo.SafetyAssessment rows are converted by RiskLabel.
        Rows with a label not listed below are left as they are.
     2. The escalation queue, review check and safety record treat a flag
        as crisis-level (escalated) when it is tier 4 or above OR carries a
        crisis-level label. The label test keeps a crisis from being missed
        if an old app build stores a 3 before the API conversion is live.

   The study tables (Study_Entry, Annotation, Gold_Label) were already on
   this scale and are not changed.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   0. dbo.SafetyAssessment has a CHECK constraint on RiskTier for the old
      0–3 scale (system-named, e.g. CK__SafetyAss__RiskT__31B762FC). Drop
      every CHECK constraint on that column; a 1–5 one is added in step 1b.
   --------------------------------------------------------------------- */
DECLARE @drop NVARCHAR(MAX) = N'';
SELECT @drop = @drop + N'ALTER TABLE dbo.SafetyAssessment DROP CONSTRAINT ' + QUOTENAME(cc.name) + N';'
FROM sys.check_constraints cc
JOIN sys.columns c ON c.object_id = cc.parent_object_id AND c.column_id = cc.parent_column_id
WHERE cc.parent_object_id = OBJECT_ID(N'dbo.SafetyAssessment') AND c.name = N'RiskTier'
  AND cc.name <> N'CK_SafetyAssessment_RiskTier_1to5';
EXEC sys.sp_executesql @drop;
GO

/* ---------------------------------------------------------------------
   1. Convert existing safety assessments by label (idempotent).
   --------------------------------------------------------------------- */
UPDATE dbo.SafetyAssessment
   SET RiskTier = CASE LOWER(LTRIM(RTRIM(RiskLabel)))
                    WHEN N'normal'            THEN 1
                    WHEN N'heated'            THEN 2
                    WHEN N'ambiguous_monitor' THEN 3
                    WHEN N'crisis'            THEN 4
                    WHEN N'extreme_abuse'     THEN 5
                    WHEN N'phq9_item9'        THEN 4
                  END
 WHERE LOWER(LTRIM(RTRIM(RiskLabel))) IN (N'normal', N'heated', N'ambiguous_monitor', N'crisis', N'extreme_abuse', N'phq9_item9')
   AND RiskTier <> CASE LOWER(LTRIM(RTRIM(RiskLabel)))
                    WHEN N'normal'            THEN 1
                    WHEN N'heated'            THEN 2
                    WHEN N'ambiguous_monitor' THEN 3
                    WHEN N'crisis'            THEN 4
                    WHEN N'extreme_abuse'     THEN 5
                    WHEN N'phq9_item9'        THEN 4
                  END;
GO

/* ---------------------------------------------------------------------
   1b. The five-tier range for new rows. Checked against existing rows
       when they all fit; otherwise (a row with an unknown label still
       below 1) it applies to new and changed rows only.
   --------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_SafetyAssessment_RiskTier_1to5')
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.SafetyAssessment WHERE RiskTier NOT BETWEEN 1 AND 5)
        ALTER TABLE dbo.SafetyAssessment WITH NOCHECK
            ADD CONSTRAINT CK_SafetyAssessment_RiskTier_1to5 CHECK (RiskTier BETWEEN 1 AND 5);
    ELSE
        ALTER TABLE dbo.SafetyAssessment WITH CHECK
            ADD CONSTRAINT CK_SafetyAssessment_RiskTier_1to5 CHECK (RiskTier BETWEEN 1 AND 5);
END;
GO

/* ---------------------------------------------------------------------
   2. Escalation: crisis-level flags (tier 4–5).
   --------------------------------------------------------------------- */
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
    WHERE (sa.RiskTier >= 4 OR LOWER(LTRIM(RTRIM(sa.RiskLabel))) IN (N'crisis', N'extreme_abuse', N'phq9_item9'))
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
                   WHERE sa.Id = @Safety_Assessment_ID
                     AND (sa.RiskTier >= 4 OR LOWER(LTRIM(RTRIM(sa.RiskLabel))) IN (N'crisis', N'extreme_abuse', N'phq9_item9')))
        THROW 51002, N'Flag not found, or not a Crisis or Extreme Abuse flag from a study couple.', 1;

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

CREATE OR ALTER VIEW study.vw_Escalation_Record
AS
SELECT sa.Id AS Safety_Assessment_ID, sa.ClientId, sa.RiskTier, sa.AssessedAt,
       r.First_Viewed_At, r.Reviewed_At,
       DATEDIFF(MINUTE, sa.AssessedAt, r.Reviewed_At) AS Minutes_To_Review
FROM dbo.SafetyAssessment sa
JOIN dbo.vw_Study_Couple c ON c.ClientId = sa.ClientId AND sa.AssessedAt >= c.Study_Start_At
LEFT JOIN study.Escalation_Review r ON r.Safety_Assessment_ID = sa.Id
WHERE sa.RiskTier >= 4 OR LOWER(LTRIM(RTRIM(sa.RiskLabel))) IN (N'crisis', N'extreme_abuse', N'phq9_item9');
GO

/* Check: every row should say ok. */
SELECT 'RiskTier allows 1-5 only' AS name,
       CASE WHEN EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_SafetyAssessment_RiskTier_1to5')
             AND NOT EXISTS (SELECT 1 FROM sys.check_constraints cc
                             JOIN sys.columns c ON c.object_id = cc.parent_object_id AND c.column_id = cc.parent_column_id
                             WHERE cc.parent_object_id = OBJECT_ID(N'dbo.SafetyAssessment') AND c.name = N'RiskTier'
                               AND cc.name <> N'CK_SafetyAssessment_RiskTier_1to5')
            THEN 'ok' ELSE 'MISSING' END AS status
UNION ALL
SELECT 'SafetyAssessment rows on the five-tier scale',
       CASE WHEN NOT EXISTS (
            SELECT 1 FROM dbo.SafetyAssessment
            WHERE (LOWER(LTRIM(RTRIM(RiskLabel))) = N'normal' AND RiskTier <> 1)
               OR (LOWER(LTRIM(RTRIM(RiskLabel))) = N'heated' AND RiskTier <> 2)
               OR (LOWER(LTRIM(RTRIM(RiskLabel))) = N'ambiguous_monitor' AND RiskTier <> 3)
               OR (LOWER(LTRIM(RTRIM(RiskLabel))) IN (N'crisis', N'phq9_item9') AND RiskTier <> 4)
               OR (LOWER(LTRIM(RTRIM(RiskLabel))) = N'extreme_abuse' AND RiskTier <> 5))
            THEN 'ok' ELSE 'MISSING' END AS status
UNION ALL
SELECT 'Escalation uses tier 4-5',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Get_Escalation_Queue')) LIKE N'%RiskTier >= 4%'
             AND OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Mark_Escalation_Reviewed')) LIKE N'%RiskTier >= 4%'
             AND OBJECT_DEFINITION(OBJECT_ID(N'study.vw_Escalation_Record')) LIKE N'%RiskTier >= 4%'
            THEN 'ok' ELSE 'MISSING' END;
GO

/* For reference: rows whose label isn't one of the known ones (left unchanged). */
SELECT RiskLabel, RiskTier, COUNT(*) AS Rows
FROM dbo.SafetyAssessment
WHERE LOWER(LTRIM(RTRIM(RiskLabel))) NOT IN (N'normal', N'heated', N'ambiguous_monitor', N'crisis', N'extreme_abuse', N'phq9_item9')
GROUP BY RiskLabel, RiskTier;
GO
