/* =====================================================================
   UCANRR study schema: patch 2026-10-31, surrogate map for de-identification
   (protocol §9.4; backs surrogate.py's SurrogateStore)
   Run as the server admin, in the study database (Pip1), AFTER
   study_patch_2026-10-28b_couple.sql. Safe to re-run.

   study.Entry_Deid, usp_Get_Entries_For_Deid and usp_Save_Deid_Text
   already exist (study_patch_2026-10-17_rating.sql /
   study_patch_2026-10-20_text_retention.sql), along with the study_deid
   role. What's missing is somewhere to keep the per-participant
   consistent name/location/org -> surrogate mapping that the
   de-identification pipeline (surrogate.py) needs so "Mark" becomes the
   same surrogate in every one of a participant's entries.

   Design, matching surrogate.py's SurrogateStore protocol:
     * Keyed only by an HMAC-SHA256 of the real value (never the value
       itself). The key lives in Key Vault, versioned (Hmac_Key_Version),
       so it can be rotated without invalidating existing rows.
     * One row per (participant, entity type, key version, hash).
     * study_deid gets EXECUTE on three wrapper procedures only (get,
       list-used, put) -- no direct table access, consistent with every
       other role in this schema.
   ===================================================================== */
SET XACT_ABORT ON;
GO

/* ---------------------------------------------------------------------
   1. study.Surrogate_Map
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'study.Surrogate_Map', N'U') IS NULL
    CREATE TABLE study.Surrogate_Map (
        Surrogate_Map_ID      BIGINT IDENTITY(1,1) CONSTRAINT PK_study_Surrogate_Map PRIMARY KEY,
        Study_Participant_ID  INT           NOT NULL
            CONSTRAINT FK_study_SM_Participant REFERENCES study.Study_Participant (Study_Participant_ID),
        Entity_Type           VARCHAR(20)   NOT NULL,   -- PERSON, LOCATION, ORG, EMAIL, PHONE, ...
        Hmac_Key_Version      INT           NOT NULL,
        Original_Hmac         BINARY(32)    NOT NULL,   -- HMAC-SHA256(secret, participant|type|normalized value)
        Role_Label             VARCHAR(30)   NULL,       -- PARTNER_1, CHILD_1, ... when known
        Surrogate_Value        NVARCHAR(200) NOT NULL,
        Created_At              DATETIME2(0)  NOT NULL CONSTRAINT DF_study_SM_Created DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_study_SM_Key UNIQUE (Study_Participant_ID, Entity_Type, Hmac_Key_Version, Original_Hmac)
    );
GO
IF OBJECT_ID(N'study.Surrogate_Map', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_study_SM_Used'
                   AND object_id = OBJECT_ID(N'study.Surrogate_Map'))
    CREATE INDEX IX_study_SM_Used ON study.Surrogate_Map (Study_Participant_ID, Entity_Type)
        INCLUDE (Surrogate_Value);
GO

/* ---------------------------------------------------------------------
   2. Wrapper procedures, one per SurrogateStore method. study_deid is
      the only caller; no other role gets access, direct or otherwise.
   --------------------------------------------------------------------- */

/* SurrogateStore.get */
CREATE OR ALTER PROCEDURE study.usp_Get_Surrogate
    @Study_Participant_ID INT,
    @Entity_Type          VARCHAR(20),
    @Hmac_Key_Version     INT,
    @Original_Hmac        BINARY(32)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT Surrogate_Value
    FROM study.Surrogate_Map
    WHERE Study_Participant_ID = @Study_Participant_ID
      AND Entity_Type = @Entity_Type
      AND Hmac_Key_Version = @Hmac_Key_Version
      AND Original_Hmac = @Original_Hmac;
END;
GO

/* SurrogateStore.used_values */
CREATE OR ALTER PROCEDURE study.usp_Get_Used_Surrogates
    @Study_Participant_ID INT,
    @Entity_Type          VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT DISTINCT Surrogate_Value
    FROM study.Surrogate_Map
    WHERE Study_Participant_ID = @Study_Participant_ID
      AND Entity_Type = @Entity_Type;
END;
GO

/* SurrogateStore.put. Idempotent: the same (participant, type, key
   version, hash) always maps to the same surrogate once set. */
CREATE OR ALTER PROCEDURE study.usp_Put_Surrogate
    @Study_Participant_ID INT,
    @Entity_Type          VARCHAR(20),
    @Hmac_Key_Version     INT,
    @Original_Hmac        BINARY(32),
    @Role_Label           VARCHAR(30) = NULL,
    @Surrogate_Value      NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF NOT EXISTS (SELECT 1 FROM study.Study_Participant WHERE Study_Participant_ID = @Study_Participant_ID)
        THROW 51002, N'Participant not found.', 1;

    MERGE study.Surrogate_Map AS t
    USING (SELECT @Study_Participant_ID AS Study_Participant_ID, @Entity_Type AS Entity_Type,
                  @Hmac_Key_Version AS Hmac_Key_Version, @Original_Hmac AS Original_Hmac) AS s
       ON t.Study_Participant_ID = s.Study_Participant_ID AND t.Entity_Type = s.Entity_Type
      AND t.Hmac_Key_Version = s.Hmac_Key_Version AND t.Original_Hmac = s.Original_Hmac
    WHEN NOT MATCHED THEN INSERT
        (Study_Participant_ID, Entity_Type, Hmac_Key_Version, Original_Hmac, Role_Label, Surrogate_Value)
        VALUES (@Study_Participant_ID, @Entity_Type, @Hmac_Key_Version, @Original_Hmac, @Role_Label, @Surrogate_Value);
    -- WHEN MATCHED: nothing to do. A row for this exact key already has its
    -- surrogate; deidentify() only calls put() after get() returned nothing.
END;
GO

/* ---------------------------------------------------------------------
   3. Withdrawal must erase surrogate rows too, same as Entry_Deid.
      CREATE OR ALTER so this stays the single source of truth for what
      withdrawal deletes; it duplicates the existing DELETE statements
      from usp_Revoke_Text_Retention and usp_Withdraw_Participant.
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE study.usp_Erase_Surrogates
    @Study_Participant_ID INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DELETE FROM study.Surrogate_Map WHERE Study_Participant_ID = @Study_Participant_ID;
END;
GO

/* ---------------------------------------------------------------------
   4. Grants. study_deid is already denied the whole schema by a prior
      patch; add EXECUTE on these four procedures only.
   --------------------------------------------------------------------- */
GRANT EXECUTE ON study.usp_Get_Surrogate        TO study_deid;
GRANT EXECUTE ON study.usp_Get_Used_Surrogates  TO study_deid;
GRANT EXECUTE ON study.usp_Put_Surrogate        TO study_deid;
GRANT EXECUTE ON study.usp_Erase_Surrogates     TO study_coordinator;
GO

/* Check: every row should say ok. */
SELECT v.name,
       CASE WHEN OBJECT_ID('study.' + v.name) IS NULL THEN 'MISSING' ELSE 'ok' END AS status
FROM (VALUES ('Surrogate_Map'), ('usp_Get_Surrogate'), ('usp_Get_Used_Surrogates'),
             ('usp_Put_Surrogate'), ('usp_Erase_Surrogates')) v(name)
UNION ALL
SELECT 'study_deid has EXECUTE on the surrogate procedures',
       CASE WHEN (
            SELECT COUNT(DISTINCT o.name)
            FROM sys.database_permissions p
            JOIN sys.database_principals r ON r.principal_id = p.grantee_principal_id
            JOIN sys.objects o ON o.object_id = p.major_id
            WHERE r.name = N'study_deid' AND p.permission_name = 'EXECUTE'
              AND o.name IN (N'usp_Get_Surrogate', N'usp_Get_Used_Surrogates', N'usp_Put_Surrogate')
            ) = 3
            THEN 'ok' ELSE 'MISSING' END;
GO
