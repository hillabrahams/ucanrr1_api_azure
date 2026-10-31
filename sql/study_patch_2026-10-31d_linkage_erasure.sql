/* =====================================================================
   UCANRR study schema: patch 2026-10-31d, scheduled erasure of the
   linking key (protocol §9.3; Attachment G, A6)

   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-31c_withdraw_erase_surrogates.sql. Safe to re-run.

   Working retention policy (not yet formally confirmed by Counsel):
   destroy study.Participant_Link (Source_Client_ID, Date_Offset_Days --
   the only thing in this schema that re-links a participant to a real
   client account) as soon as it stops being needed:
     * Withdrawn participants: immediately. usp_Withdraw_Participant
       already deletes their entries; the link protects nothing further
       once Delete_Linkage wasn't used at withdrawal time.
     * Everyone else: once the study is closed (so no more entries will
       ever be ingested) AND every entry they have with kept text has
       been de-identified (de-identification needs Date_Offset_Days to
       shift dates) AND a grace period has passed since close. Default
       90 days, the conservative end of the 30-90-day range.

   One eligibility rule, used by both the read-only candidate list and
   the actual erasure, so they can't drift apart the way
   usp_Erase_Surrogates and its callers did (patch 2026-10-31c).
   Nothing else in the schema changes: this only ever deletes
   Participant_Link rows.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Single source of truth for "is this participant's link eligible
      for erasure right now".
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
          )
);
GO

/* ---------------------------------------------------------------------
   2. Review before erasing.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Get_Linkage_Erasure_Candidates
    @Grace_Days INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Must be run by a study coordinator.', 1;
    IF @Grace_Days < 30
        THROW 51033, N'Grace_Days must be at least 30.', 1;

    SELECT sp.Study_Participant_ID, sp.Couple_ID, pl.Source_Client_ID, f.Reason
    FROM study.fn_linkage_erasure_eligible(@Grace_Days) f
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = f.Study_Participant_ID
    JOIN study.Participant_Link pl ON pl.Study_Participant_ID = sp.Study_Participant_ID
    ORDER BY sp.Study_Participant_ID;
END;
GO

/* ---------------------------------------------------------------------
   3. Erase every currently eligible link. Re-validates with the same
      function rather than trusting a list the caller already fetched,
      so nothing can be erased on stale eligibility.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Erase_Eligible_Linkage
    @Grace_Days INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Must be run by a study coordinator.', 1;
    IF @Grace_Days < 30
        THROW 51033, N'Grace_Days must be at least 30.', 1;

    DECLARE @Erased TABLE (Study_Participant_ID INT);

    BEGIN TRAN;
        DELETE pl
        OUTPUT deleted.Study_Participant_ID INTO @Erased
        FROM study.Participant_Link pl
        JOIN study.fn_linkage_erasure_eligible(@Grace_Days) f ON f.Study_Participant_ID = pl.Study_Participant_ID;

        INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
        SELECT 'ERASE_LINKAGE', Study_Participant_ID, CONCAT(N'grace days ', @Grace_Days) FROM @Erased;
    COMMIT;

    SELECT Study_Participant_ID FROM @Erased ORDER BY Study_Participant_ID;
END;
GO

GRANT EXECUTE ON study.usp_Get_Linkage_Erasure_Candidates TO study_coordinator;
GRANT EXECUTE ON study.usp_Erase_Eligible_Linkage        TO study_coordinator;
GO

/* ---------------------------------------------------------------------
   4. ERASE_LINKAGE is a new Access_Audit action.
   --------------------------------------------------------------------- */
IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_study_Audit_Action')
    ALTER TABLE study.Access_Audit DROP CONSTRAINT CK_study_Audit_Action;
GO
ALTER TABLE study.Access_Audit ADD CONSTRAINT CK_study_Audit_Action CHECK (Action IN (
    'SESSION_START','SESSION_DENIED','VIEW_QUEUE','VIEW_ENTRY','VIEW_ENTRY_DENIED',
    'SAVE_ANNOTATION','SAVE_DENIED','SAVE_GOLD','INGEST','INGEST_SKIPPED',
    'ENROLL','ASSIGN','REVOKE_ASSIGNMENT','REGISTER_AGREEMENT','REVOKE_AGREEMENT',
    'REGISTER_ANNOTATOR','PAUSE','RESUME','WITHDRAW',
    'CLOSE_STUDY','RECORD_TEXT_RETENTION','REVOKE_TEXT_RETENTION','DEID_READ','DEID_SAVE',
    'VIEW_ESCALATIONS','REVIEW_ESCALATION',
    'EXPORT','ERASE_LINKAGE'));
GO

/* Check: every row should say ok. */
SELECT 'fn_linkage_erasure_eligible' AS name,
       CASE WHEN OBJECT_ID(N'study.fn_linkage_erasure_eligible', N'IF') IS NULL THEN 'MISSING' ELSE 'ok' END AS status
UNION ALL
SELECT 'usp_Get_Linkage_Erasure_Candidates',
       CASE WHEN OBJECT_ID(N'study.usp_Get_Linkage_Erasure_Candidates') IS NULL THEN 'MISSING' ELSE 'ok' END
UNION ALL
SELECT 'usp_Erase_Eligible_Linkage',
       CASE WHEN OBJECT_ID(N'study.usp_Erase_Eligible_Linkage') IS NULL THEN 'MISSING' ELSE 'ok' END
UNION ALL
SELECT 'ERASE_LINKAGE is an allowed Access_Audit action',
       CASE WHEN (SELECT definition FROM sys.check_constraints WHERE name = N'CK_study_Audit_Action') LIKE N'%ERASE_LINKAGE%'
            THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'study_coordinator has EXECUTE on both new procedures',
       CASE WHEN (
            SELECT COUNT(DISTINCT o.name)
            FROM sys.database_permissions p
            JOIN sys.database_principals r ON r.principal_id = p.grantee_principal_id
            JOIN sys.objects o ON o.object_id = p.major_id
            WHERE r.name = N'study_coordinator' AND p.permission_name = 'EXECUTE'
              AND o.name IN (N'usp_Get_Linkage_Erasure_Candidates', N'usp_Erase_Eligible_Linkage')
            ) = 2 THEN 'ok' ELSE 'MISSING' END;
GO
