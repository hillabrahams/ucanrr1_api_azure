/* =====================================================================
   Minimal stand-ins for the main UCANRR tables that the study patches
   read (dbo.Roles, dbo.Therapist, dbo.Client, dbo.SafetyAssessment).
   For an EMPTY dev database only, so the study tests can run without a
   copy of production. Never run this on the production database.
   Run before study_patch_2026-10-17b_escalation.sql.
   ===================================================================== */
SET NOCOUNT ON;
IF OBJECT_ID(N'dbo.Roles', N'U') IS NULL
    CREATE TABLE dbo.Roles (
        RoleID INT IDENTITY(1,1) PRIMARY KEY, RoleName NVARCHAR(100) NOT NULL,
        RedirectURL NVARCHAR(400) NULL, Description NVARCHAR(400) NULL,
        IsActive BIT NOT NULL DEFAULT 1, CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
IF OBJECT_ID(N'dbo.Therapist', N'U') IS NULL
    CREATE TABLE dbo.Therapist (Id INT PRIMARY KEY, TherapistUserName NVARCHAR(200) NULL);
IF OBJECT_ID(N'dbo.Client', N'U') IS NULL
    CREATE TABLE dbo.Client (Id INT PRIMARY KEY, TherapistID INT NULL,
        Client1FirstName NVARCHAR(100) NULL, Client2FirstName NVARCHAR(100) NULL);
IF OBJECT_ID(N'dbo.SafetyAssessment', N'U') IS NULL
    CREATE TABLE dbo.SafetyAssessment (
        Id INT IDENTITY(1,1) PRIMARY KEY, EventId INT NULL, ClientId INT NOT NULL,
        IsClient1 BIT NOT NULL DEFAULT 0, IsClient2 BIT NOT NULL DEFAULT 0,
        RiskTier INT NOT NULL, RiskLabel NVARCHAR(100) NOT NULL,
        HasSuicidalIdeation BIT NOT NULL DEFAULT 0, HasSelfHarm BIT NOT NULL DEFAULT 0,
        HasOtherHarm BIT NOT NULL DEFAULT 0, HasExtremeAbuse BIT NOT NULL DEFAULT 0,
        HasHeatedArgument BIT NOT NULL DEFAULT 0, HasCrisisLanguage BIT NOT NULL DEFAULT 0,
        MentionsSubstanceUse BIT NOT NULL DEFAULT 0, MentionsWeaponAccess BIT NOT NULL DEFAULT 0,
        MentionsChildSafetyConcern BIT NOT NULL DEFAULT 0, AmbiguousLethalCuriosity BIT NOT NULL DEFAULT 0,
        PartnerSharePolicy NVARCHAR(50) NOT NULL DEFAULT N'warn', TherapistSharePolicy NVARCHAR(50) NOT NULL DEFAULT N'allow',
        ShowCrisisBanner BIT NOT NULL DEFAULT 0, ShowCrisisResources BIT NOT NULL DEFAULT 0,
        SuggestedUiFlow NVARCHAR(100) NOT NULL DEFAULT N'none', IsUrgentForTherapist BIT NOT NULL DEFAULT 0,
        NotesForTherapist NVARCHAR(MAX) NULL, Explanation NVARCHAR(MAX) NOT NULL DEFAULT N'',
        ApiVersion NVARCHAR(20) NOT NULL DEFAULT N'1.2.0',
        TherapistAcknowledgedAt DATETIME2 NULL, TherapistAcknowledgedById INT NULL,
        AssessedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
GO
