/* =====================================================================
   UCANRR study schema: patch 2026-10-31b, coordinator-mediated
   participant data export (protocol §9.6; Attachment G, A8)

   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-31_surrogate_map.sql. Safe to re-run.

   A8 asks whether participants have view, export and delete controls.
   Delete already exists (usp_Withdraw_Participant). This patch adds the
   export half: participants don't get a self-service portal, so a study
   coordinator assembles and hands over a participant's own study data on
   request. The export covers exactly what withdrawal deletes --
   Study_Participant, Study_Entry, Entry_Deid, Annotation, Gold_Label --
   plus that participant's own Access_Audit trail, so the two stay
   symmetric. It never includes Participant_Link.Date_Offset_Days: that's
   the de-identification secret, not the participant's data, and coord
   endpoints already keep it out of every response (see coord_overview).

   Only one new procedure is needed. study_coordinator already has
   GRANT SELECT ON SCHEMA::study, so the export query itself is plain
   SELECTs run from the API (study_router.py), matching coord_overview.
   What the caller can't do directly is write the audit row -- INSERT
   isn't granted on the schema, only SELECT -- so a small proc exists
   for that one step, the same ownership-chaining pattern as every other
   mutation here.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. EXPORT is a new Access_Audit action.
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
    'EXPORT'));
GO

/* ---------------------------------------------------------------------
   2. Validate + log. The actual SELECTs run afterward from the API,
      under the caller's own schema-wide SELECT grant.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Record_Export_Access
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF IS_MEMBER(N'study_coordinator') <> 1
        THROW 51008, N'Export must be run by a study coordinator.', 1;
    IF NOT EXISTS (SELECT 1 FROM study.Study_Participant WHERE Study_Participant_ID = @Study_Participant_ID)
        THROW 51002, N'Participant not found.', 1;

    INSERT study.Access_Audit (Action, Study_Participant_ID, Detail)
    VALUES ('EXPORT', @Study_Participant_ID, N'participant data export');
END;
GO

GRANT EXECUTE ON study.usp_Record_Export_Access TO study_coordinator;
GO

/* Check: every row should say ok. */
SELECT 'usp_Record_Export_Access' AS name,
       CASE WHEN OBJECT_ID('study.usp_Record_Export_Access') IS NULL THEN 'MISSING' ELSE 'ok' END AS status
UNION ALL
SELECT 'EXPORT is an allowed Access_Audit action',
       CASE WHEN (SELECT definition FROM sys.check_constraints WHERE name = N'CK_study_Audit_Action') LIKE N'%EXPORT%'
            THEN 'ok' ELSE 'MISSING' END
UNION ALL
SELECT 'study_coordinator has EXECUTE on usp_Record_Export_Access',
       CASE WHEN EXISTS (
            SELECT 1 FROM sys.database_permissions p
            JOIN sys.database_principals r ON r.principal_id = p.grantee_principal_id
            JOIN sys.objects o ON o.object_id = p.major_id
            WHERE r.name = N'study_coordinator' AND p.permission_name = 'EXECUTE'
              AND o.name = N'usp_Record_Export_Access'
            ) THEN 'ok' ELSE 'MISSING' END;
GO
