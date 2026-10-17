/* =====================================================================
   UCANRR study annotation schema: patch 2026-10-07
   Run in Pip1 as the server admin, AFTER ucanrr_study_annotation_schema.sql.
   Safe to re-run.

   Adds what study_router.py calls but the 2026-10-06 schema does not create:
     1. study.Access_Audit.Actor_User_ID, filled from SESSION_CONTEXT
        'actor_user_id' (set read-only by the API on every connection),
        and vw_Audit_Daily grouped by it          -> monitor page
     2. study.vw_Label_Pairs                      -> monitor page
     3. study.usp_Get_Adjudication_Queue          -> adjudicate page
     4. study.usp_Get_Gold_Label                  -> adjudicate page
   And changes what it reads differently:
     5. study.usp_Resolve_Annotator also returns Annotator_Type and
        Can_View_Restricted                       -> annotate + adjudicate
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. Who acted: AuthorizedUsers.UserID of the signed-in person.
      A default (not an UPDATE) keeps the append-only trigger intact;
      existing rows stay NULL. Ingestion run directly in SQL has no
      session context, so its rows are NULL too.
   --------------------------------------------------------------------- */
IF COL_LENGTH('study.Access_Audit', 'Actor_User_ID') IS NULL
    ALTER TABLE study.Access_Audit
        ADD Actor_User_ID INT NULL
            CONSTRAINT DF_study_Audit_Actor DEFAULT (TRY_CONVERT(INT, SESSION_CONTEXT(N'actor_user_id')));
GO

/* Weekly audit review: activity per person per day, with denied
   events broken out. */
CREATE OR ALTER VIEW study.vw_Audit_Daily
AS
SELECT CAST(Event_At AS DATE) AS Event_Date,
       Annotator_ID,
       Actor_User_ID,
       Db_User,
       SUM(CASE WHEN Action = 'VIEW_ENTRY' THEN 1 ELSE 0 END)                         AS Entries_Viewed,
       SUM(CASE WHEN Action = 'SAVE_ANNOTATION' THEN 1 ELSE 0 END)                    AS Annotations_Saved,
       SUM(CASE WHEN Action IN ('VIEW_ENTRY_DENIED','SAVE_DENIED','SESSION_DENIED') THEN 1 ELSE 0 END) AS Denied_Events,
       COUNT(DISTINCT Study_Participant_ID)                                           AS Participants_Touched
FROM study.Access_Audit
GROUP BY CAST(Event_At AS DATE), Annotator_ID, Actor_User_ID, Db_User;
GO

/* ---------------------------------------------------------------------
   2. Inter-rater pairs: one row per entry that has a current label from
      both a treating therapist (T_) and a second labeler (L_). Labels
      only, never text. Read by the coordinator connection, which RLS
      lets see every annotation.
   --------------------------------------------------------------------- */
CREATE OR ALTER VIEW study.vw_Label_Pairs
AS
WITH cur AS (
    SELECT a.Study_Entry_ID, an.Annotator_Type,
           a.IsNeglect, a.IsRepair, a.IsBid, a.IsCare, a.IsShared, a.Sentiment, a.Safety_Tier,
           -- One label per type per entry, should an entry ever have two of a type.
           ROW_NUMBER() OVER (PARTITION BY a.Study_Entry_ID, an.Annotator_Type ORDER BY a.Annotator_ID) AS rn
    FROM study.Annotation a
    JOIN study.Annotator an ON an.Annotator_ID = a.Annotator_ID
    WHERE a.Is_Current = 1
      AND an.Annotator_Type IN ('TREATING_THERAPIST', 'SECOND_LABELER')
)
SELECT e.Study_Entry_ID,
       e.Study_Participant_ID,
       t.IsNeglect AS T_IsNeglect, t.IsRepair AS T_IsRepair, t.IsBid AS T_IsBid,
       t.IsCare    AS T_IsCare,    t.IsShared AS T_IsShared,
       t.Sentiment AS T_Sentiment, t.Safety_Tier AS T_Safety_Tier,
       l.IsNeglect AS L_IsNeglect, l.IsRepair AS L_IsRepair, l.IsBid AS L_IsBid,
       l.IsCare    AS L_IsCare,    l.IsShared AS L_IsShared,
       l.Sentiment AS L_Sentiment, l.Safety_Tier AS L_Safety_Tier
FROM study.Study_Entry e
JOIN cur t ON t.Study_Entry_ID = e.Study_Entry_ID AND t.Annotator_Type = 'TREATING_THERAPIST' AND t.rn = 1
JOIN cur l ON l.Study_Entry_ID = e.Study_Entry_ID AND l.Annotator_Type = 'SECOND_LABELER'     AND l.rn = 1;
GO

/* ---------------------------------------------------------------------
   3. Adjudication queue: entries the adjudicator can see (RLS) that have
      at least two current labels from other annotators, the same rule
      usp_Save_Gold_Label enforces. Differences and Crisis-line splits
      come first.
   --------------------------------------------------------------------- */
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

    SELECT q.Study_Entry_ID, q.Study_Participant_ID, q.Entry_Created_At, q.Safety_Tier_At_Capture,
           q.Labels, q.Min_Tier, q.Max_Tier, q.Is_Unanimous, q.Has_Gold
    FROM (
        SELECT e.Study_Entry_ID, e.Study_Participant_ID, e.Entry_Created_At, e.Safety_Tier_At_Capture,
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
             q.Max_Tier DESC, q.Entry_Created_At, q.Study_Entry_ID;
END;
GO

/* ---------------------------------------------------------------------
   4. Gold label for one entry. Gold_Label has no RLS of its own, so
      visibility is checked through Study_Entry, which has.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Get_Gold_Label
    @Study_Entry_ID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Me INT = CAST(SESSION_CONTEXT(N'annotator_id') AS INT);
    IF @Me IS NULL THROW 51001, N'annotator_id not set in session context.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Annotator
                   WHERE Annotator_ID = @Me AND Annotator_Type = 'ADJUDICATOR' AND Is_Active = 1)
        THROW 51005, N'Only an active adjudicator can view gold labels.', 1;

    IF NOT EXISTS (SELECT 1 FROM study.Study_Entry WHERE Study_Entry_ID = @Study_Entry_ID)
        RETURN;  -- empty result: missing or not visible

    SELECT Study_Entry_ID, Adjudicator_ID, Method,
           IsNeglect, IsRepair, IsBid, IsCare, IsShared, Sentiment, Safety_Tier,
           Created_At, Updated_At
    FROM study.Gold_Label
    WHERE Study_Entry_ID = @Study_Entry_ID;
END;
GO

/* ---------------------------------------------------------------------
   5. Session start: the API also needs the annotator's type and
      restricted-access flag, to check the type matches the person's
      role. Same rules and audit rows as before; two more columns.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Resolve_Annotator
    @Source_User_ID NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Annotator_ID INT, @Annotator_Type VARCHAR(20), @Can_View_Restricted BIT;

    SELECT @Annotator_ID = a.Annotator_ID, @Annotator_Type = a.Annotator_Type,
           @Can_View_Restricted = a.Can_View_Restricted
    FROM study.Annotator a
    JOIN study.Privacy_Agreement pa ON pa.Agreement_ID = a.Agreement_ID
    WHERE a.Source_User_ID = @Source_User_ID
      AND a.Is_Active = 1
      AND pa.Revoked_At IS NULL
      AND (pa.Expires_At IS NULL OR pa.Expires_At > SYSUTCDATETIME());

    IF @Annotator_ID IS NULL
    BEGIN
        INSERT study.Access_Audit (Action, Detail) VALUES ('SESSION_DENIED', N'No active annotator/agreement for user');
        SELECT CAST(NULL AS INT) AS Annotator_ID, CAST(NULL AS VARCHAR(20)) AS Annotator_Type,
               CAST(NULL AS BIT) AS Can_View_Restricted;
        RETURN;
    END;

    INSERT study.Access_Audit (Annotator_ID, Action) VALUES (@Annotator_ID, 'SESSION_START');
    SELECT @Annotator_ID AS Annotator_ID, @Annotator_Type AS Annotator_Type,
           @Can_View_Restricted AS Can_View_Restricted;
END;
GO

/* ---------------------------------------------------------------------
   Permissions: the app runs the two procedures; the coordinator's
   schema-level SELECT already covers both views. study_app's DENY on
   the schema still blocks it from reading the views directly.
   --------------------------------------------------------------------- */
GRANT EXECUTE ON study.usp_Get_Adjudication_Queue TO study_app;
GRANT EXECUTE ON study.usp_Get_Gold_Label         TO study_app;
GO

/* Check: every row should say ok. */
SELECT v.name,
       CASE WHEN OBJECT_ID('study.' + v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('usp_Get_Adjudication_Queue'), ('usp_Get_Gold_Label'), ('vw_Label_Pairs'), ('vw_Audit_Daily')) v(name)
UNION ALL
SELECT 'Access_Audit.Actor_User_ID',
       CASE WHEN COL_LENGTH('study.Access_Audit', 'Actor_User_ID') IS NULL THEN 'MISSING' ELSE 'ok' END;
GO
