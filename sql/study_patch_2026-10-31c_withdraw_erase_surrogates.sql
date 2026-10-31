/* =====================================================================
   UCANRR study schema: patch 2026-10-31c, withdrawal and B3 revocation
   erase surrogate rows too

   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-31_surrogate_map.sql. Safe to re-run.

   Bug: study_patch_2026-10-31_surrogate_map.sql's comment says
   usp_Erase_Surrogates "duplicates the existing DELETE statements from
   usp_Revoke_Text_Retention and usp_Withdraw_Participant" -- the intent
   was for those two procedures to also erase study.Surrogate_Map, the
   same way they already erase study.Entry_Deid. Neither one was ever
   updated to do it. Until now, withdrawing a participant (or revoking
   their B3 permission to keep text) deleted their raw and de-identified
   entries but left their HMAC-keyed name/location/org surrogate mapping
   behind indefinitely -- exactly the kind of leftover, keyed-but-still-
   linkable record A6 (retention) is supposed to rule out.

   Fix: add the same DELETE, scoped the same way as the Entry_Deid
   DELETE immediately above it in each procedure, direct against
   study.Surrogate_Map rather than a call to usp_Erase_Surrogates --
   both procedures already delete straight from study.Gold_Label,
   study.Annotation and study.Entry_Deid in this same set-based style
   (ownership chaining covers the missing table-level grant, same as
   those three), so this just extends the existing pattern instead of
   introducing a different one.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Withdrawal: whole couple, same as every other table here.
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

/* ---------------------------------------------------------------------
   2. B3 revocation: single participant, same as the other DELETEs here.
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

        DELETE FROM study.Surrogate_Map WHERE Study_Participant_ID = @Study_Participant_ID;

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

/* Check: every row should say ok. */
SELECT 'usp_Withdraw_Participant erases Surrogate_Map' AS name,
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Withdraw_Participant')) LIKE N'%DELETE FROM study.Surrogate_Map%'
            THEN 'ok' ELSE 'MISSING' END AS status
UNION ALL
SELECT 'usp_Revoke_Text_Retention erases Surrogate_Map',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'study.usp_Revoke_Text_Retention')) LIKE N'%DELETE FROM study.Surrogate_Map%'
            THEN 'ok' ELSE 'MISSING' END;
GO
