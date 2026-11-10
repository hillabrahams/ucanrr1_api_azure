/* =====================================================================
   UCANRR study schema: patch 2026-11-09, keep the study period after
   the account link is deleted (protocol §8.2)
   Run as the server admin, in the study database, AFTER
   study_patch_2026-10-31g_reminder_delivery.sql. Safe to re-run.

   dbo.vw_Study_Couple was built only from study.Participant_Link. Three
   things delete those links: Withdraw with "delete the link" ticked (the
   default), usp_Erase_Eligible_Linkage, and erasure at study close.
   Once a couple's links were gone it dropped out of the view, so:
     - the main API stopped hiding its study-period safety flags, and
       Crisis and Extreme Abuse flags reviewed only by the escalation
       clinician appeared on the therapist dashboard;
     - its unreviewed flags dropped out of the escalation queue.

     1. dbo.Study_Client_Period: the client ID and when its study period
        started, nothing else. It holds no participant ID, so it does not
        undo the de-linking.
     2. Filled from the current links, and by a trigger whenever links
        are deleted.
     3. dbo.vw_Study_Couple reads the live links and this table, so a
        couple stays a study couple after its links are deleted.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Study period per client, kept after the links are deleted.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'dbo.Study_Client_Period', N'U') IS NULL
    CREATE TABLE dbo.Study_Client_Period (
        ClientId        INT           NOT NULL CONSTRAINT PK_Study_Client_Period PRIMARY KEY,
        Study_Start_At  DATETIME2(0)  NOT NULL,
        Recorded_At     DATETIME2(0)  NOT NULL CONSTRAINT DF_Study_Client_Period_Recorded DEFAULT SYSUTCDATETIME()
    );
GO

/* ---------------------------------------------------------------------
   2. Keep the period when links are deleted. Both partners' links go in
      one statement on withdrawal, so take the earliest consent per client
      across the deleted rows, and never move an existing start later.
   --------------------------------------------------------------------- */
CREATE OR ALTER TRIGGER study.trg_Participant_Link_Keep_Period
ON study.Participant_Link
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    MERGE dbo.Study_Client_Period AS t
    USING (
        SELECT TRY_CONVERT(INT, LEFT(d.Source_Client_ID, CHARINDEX('-', d.Source_Client_ID) - 1)) AS ClientId,
               MIN(sp.Research_Consent_At) AS Study_Start_At
        FROM deleted d
        JOIN study.Study_Participant sp ON sp.Study_Participant_ID = d.Study_Participant_ID
        WHERE CHARINDEX('-', d.Source_Client_ID) > 1
        GROUP BY TRY_CONVERT(INT, LEFT(d.Source_Client_ID, CHARINDEX('-', d.Source_Client_ID) - 1))
    ) AS s
    ON t.ClientId = s.ClientId
    WHEN MATCHED AND s.Study_Start_At < t.Study_Start_At THEN
        UPDATE SET Study_Start_At = s.Study_Start_At
    WHEN NOT MATCHED BY TARGET AND s.ClientId IS NOT NULL THEN
        INSERT (ClientId, Study_Start_At) VALUES (s.ClientId, s.Study_Start_At);
END;
GO

/* ---------------------------------------------------------------------
   3. Study couples: live links, plus couples whose links were deleted.
   --------------------------------------------------------------------- */
CREATE OR ALTER VIEW dbo.vw_Study_Couple
AS
SELECT ClientId, MIN(Study_Start_At) AS Study_Start_At
FROM (
    SELECT TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1)) AS ClientId,
           sp.Research_Consent_At AS Study_Start_At
    FROM study.Participant_Link pl
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = pl.Study_Participant_ID
    WHERE CHARINDEX('-', pl.Source_Client_ID) > 1
    UNION ALL
    SELECT ClientId, Study_Start_At FROM dbo.Study_Client_Period
) c
WHERE ClientId IS NOT NULL
GROUP BY ClientId;
GO

/* Couples whose links still exist: record them now too, so the table
   holds every study couple, not only the ones withdrawn from here on.
   Couples whose links were deleted before this patch can't be recovered;
   add them by hand from the coordinator's records if there are any. */
MERGE dbo.Study_Client_Period AS t
USING (
    SELECT TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1)) AS ClientId,
           MIN(sp.Research_Consent_At) AS Study_Start_At
    FROM study.Participant_Link pl
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = pl.Study_Participant_ID
    WHERE CHARINDEX('-', pl.Source_Client_ID) > 1
    GROUP BY TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1))
) AS s
ON t.ClientId = s.ClientId
WHEN MATCHED AND s.Study_Start_At < t.Study_Start_At THEN
    UPDATE SET Study_Start_At = s.Study_Start_At
WHEN NOT MATCHED BY TARGET AND s.ClientId IS NOT NULL THEN
    INSERT (ClientId, Study_Start_At) VALUES (s.ClientId, s.Study_Start_At);
GO

/* Check: every row should say ok. */
SELECT v.name,
       CASE WHEN OBJECT_ID(v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('dbo.Study_Client_Period'), ('study.trg_Participant_Link_Keep_Period'), ('dbo.vw_Study_Couple')) v(name)
UNION ALL
SELECT 'vw_Study_Couple reads dbo.Study_Client_Period',
       CASE WHEN OBJECT_DEFINITION(OBJECT_ID(N'dbo.vw_Study_Couple')) LIKE N'%Study_Client_Period%' THEN 'ok' ELSE 'MISSING' END;
GO
