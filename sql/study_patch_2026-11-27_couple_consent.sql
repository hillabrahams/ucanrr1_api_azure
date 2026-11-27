/* =====================================================================
   UCANRR study schema: patch 2026-11-27, the couple's consent date
   (the later partner's research consent)
   Run as the server admin, in the study database, AFTER
   study_patch_2026-11-09_study_period.sql. Safe to re-run.

   A couple's first study session can't be before the couple consented
   to the study. A couple has consented once BOTH partners have, so that
   is the later partner's Research_Consent_At, as the study procedures
   already use (MAX over the couple's Study_Participant rows).

   dbo.vw_Study_Couple can't be used for this: its Study_Start_At is the
   EARLIER partner's consent, on purpose, so safety flags are hidden from
   the earliest moment. It stays as it is.

     1. dbo.vw_Study_Couple_Consent: one row per study couple (the same
        couples as dbo.vw_Study_Couple) with Couple_Consent_At, the later
        partner's consent. The main API's login is denied the study
        schema and reads this dbo view, like dbo.vw_Study_Couple.
     2. Once a couple's links are deleted (withdrawal, linkage erasure,
        study close), only dbo.Study_Client_Period is left, which keeps
        the earlier consent. The view falls back to that for those
        couples.
   ===================================================================== */
SET XACT_ABORT ON;
GO

CREATE OR ALTER VIEW dbo.vw_Study_Couple_Consent
AS
SELECT c.ClientId,
       COALESCE(l.Couple_Consent_At, c.Study_Start_At) AS Couple_Consent_At
FROM dbo.vw_Study_Couple c
LEFT JOIN (
    SELECT TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1)) AS ClientId,
           MAX(sp.Research_Consent_At) AS Couple_Consent_At
    FROM study.Participant_Link pl
    JOIN study.Study_Participant sp ON sp.Study_Participant_ID = pl.Study_Participant_ID
    WHERE CHARINDEX('-', pl.Source_Client_ID) > 1
    GROUP BY TRY_CONVERT(INT, LEFT(pl.Source_Client_ID, CHARINDEX('-', pl.Source_Client_ID) - 1))
) l ON l.ClientId = c.ClientId;
GO
-- The main API's login reads dbo.vw_Study_Couple_Consent like the other dbo objects.

/* Check: every row should say ok. */
SELECT v.name,
       CASE WHEN OBJECT_ID(v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('dbo.vw_Study_Couple'), ('dbo.vw_Study_Couple_Consent')) v(name)
UNION ALL
SELECT 'vw_Study_Couple_Consent has every study couple',
       CASE WHEN (SELECT COUNT(*) FROM dbo.vw_Study_Couple) = (SELECT COUNT(*) FROM dbo.vw_Study_Couple_Consent)
            THEN 'ok' ELSE 'MISMATCH' END
UNION ALL
SELECT 'Couple consent is never before the earlier partner''s',
       CASE WHEN NOT EXISTS (SELECT 1 FROM dbo.vw_Study_Couple c
                             JOIN dbo.vw_Study_Couple_Consent k ON k.ClientId = c.ClientId
                             WHERE k.Couple_Consent_At < c.Study_Start_At)
            THEN 'ok' ELSE 'WRONG' END;
GO
