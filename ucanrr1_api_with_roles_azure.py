from fastapi import FastAPI, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List
import pyodbc
from datetime import datetime
from contextlib import contextmanager
import os
import json
import firebase_admin
from firebase_admin import credentials, auth as firebase_auth

# Load .env file when running locally (no-op if file doesn't exist)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = FastAPI(title="UCANRR1 API", version="2.0.0")

# ==================== FIREBASE INITIALIZATION ====================
# Supports two methods:
#   1. FIREBASE_SERVICE_ACCOUNT_JSON env var - JSON string (recommended for Azure)
#   2. FIREBASE_SERVICE_ACCOUNT_PATH env var  - path to JSON file (local dev)

_firebase_json_str = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
if _firebase_json_str:
    _cred = credentials.Certificate(json.loads(_firebase_json_str))
    firebase_admin.initialize_app(_cred)
else:
    _firebase_service_account = os.environ.get(
        "FIREBASE_SERVICE_ACCOUNT_PATH",
        os.path.join(os.path.dirname(__file__), "ucanrr-firebase-adminsdk-fbsvc-29b05a8759.json")
    )
    if os.path.exists(_firebase_service_account):
        _cred = credentials.Certificate(_firebase_service_account)
        firebase_admin.initialize_app(_cred)
    else:
        raise RuntimeError(
            "Firebase credentials not configured. Set FIREBASE_SERVICE_ACCOUNT_JSON "
            "(JSON string) or FIREBASE_SERVICE_ACCOUNT_PATH (file path) environment variable."
        )

# ==================== CORS CONFIGURATION ====================

_raw_origins = os.environ.get(
    "ALLOWED_ORIGINS",
    "http://localhost:8000,https://ucanrr.com,https://www.ucanrr.com"
)
_allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ==================== DATABASE CONFIGURATION ====================
# Azure: set AZURE_SQL_CONNECTION_STRING in App Service > Configuration > Application settings
# Full ODBC connection string, e.g.:
#   DRIVER={ODBC Driver 18 for SQL Server};SERVER=<server>.database.windows.net;
#   DATABASE=<db>;UID=<user>;PWD=<password>;
#   Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;

CONNECTION_STRING = os.environ.get(
    "AZURE_SQL_CONNECTION_STRING",
    "Driver={ODBC Driver 17 for SQL Server};Server=tcp:Pip1.database.windows.net,1433;Database=Pip1;Uid=hillabrahams@Pip1;Pwd=Abhabhabh0919?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = pyodbc.connect(CONNECTION_STRING)
    try:
        yield conn
    finally:
        conn.close()

# ==================== PYDANTIC MODELS ====================

# Role Models
class RoleBase(BaseModel):
    RoleName: str
    RedirectURL: Optional[str] = None
    Description: Optional[str] = None
    IsActive: Optional[bool] = True

class RoleCreate(RoleBase):
    pass

class RoleUpdate(BaseModel):
    RoleName: Optional[str] = None
    RedirectURL: Optional[str] = None
    Description: Optional[str] = None
    IsActive: Optional[bool] = None

class Role(RoleBase):
    RoleID: int
    CreatedAt: Optional[datetime] = None

    model_config = {"from_attributes": True}

# AuthorizedUser Models (Updated with RoleID)
class AuthorizedUserBase(BaseModel):
    Email: EmailStr
    FullName: Optional[str] = None
    IsActive: Optional[bool] = True
    RoleID: Optional[int] = None

class AuthorizedUserCreate(AuthorizedUserBase):
    pass

class AuthorizedUserUpdate(BaseModel):
    Email: Optional[EmailStr] = None
    FullName: Optional[str] = None
    IsActive: Optional[bool] = None
    LastLogin: Optional[datetime] = None
    RoleID: Optional[int] = None

class AuthorizedUser(AuthorizedUserBase):
    UserID: int
    CreatedAt: Optional[datetime] = None
    LastLogin: Optional[datetime] = None

    model_config = {"from_attributes": True}

# AuthorizedUser with Role Details
class AuthorizedUserWithRole(BaseModel):
    UserID: int
    Email: str
    FullName: Optional[str] = None
    IsActive: Optional[bool] = None
    CreatedAt: Optional[datetime] = None
    LastLogin: Optional[datetime] = None
    RoleID: Optional[int] = None
    RoleName: Optional[str] = None
    RedirectURL: Optional[str] = None

    model_config = {"from_attributes": True}

# Therapist Models
class TherapistBase(BaseModel):
    TherapistUserName: str
    ThearapistPassword: str
    ClientId: int

class TherapistCreate(TherapistBase):
    pass

class TherapistUpdate(BaseModel):
    TherapistUserName: Optional[str] = None
    ThearapistPassword: Optional[str] = None
    ClientId: Optional[int] = None

class Therapist(TherapistBase):
    Id: int
    CreatedDate: Optional[datetime] = None
    UpdatedDate: Optional[datetime] = None

    model_config = {"from_attributes": True}

# Client Models
class ClientBase(BaseModel):
    TherapistID: int
    Client1FirstName: Optional[str] = None
    Client1UserName: str
    Client1Phone: str
    Client2FirstName: Optional[str] = None
    Client2UserName: str
    Client2Phone: str

class ClientCreate(ClientBase):
    pass

class ClientUpdate(BaseModel):
    TherapistID: Optional[int] = None
    Client1FirstName: Optional[str] = None
    Client1UserName: Optional[str] = None
    Client1Phone: Optional[str] = None
    Client2FirstName: Optional[str] = None
    Client2UserName: Optional[str] = None
    Client2Phone: Optional[str] = None

class Client(ClientBase):
    Id: int
    CreatedDate: Optional[datetime] = None
    UpdatedDate: Optional[datetime] = None

    model_config = {"from_attributes": True}

# Event Models
class EventBase(BaseModel):
    ClientId: int
    IsClient1: bool = False
    IsClient2: bool = False
    Score: int
    IsNeglect: Optional[bool] = False
    IsRepair: Optional[bool] = False
    IsShared: Optional[bool] = False
    IsBid: Optional[bool] = None
    IsSce: Optional[bool] = None
    IsText: bool
    IsAudio: Optional[bool] = None
    IsVideo: Optional[bool] = None
    Affect: Optional[int] = None
    IsSharedDate: Optional[datetime] = None
    EventDate: datetime
    TherapistNotes: Optional[str] = None

class EventCreate(EventBase):
    pass

class EventUpdate(BaseModel):
    ClientId: Optional[int] = None
    IsClient1: Optional[bool] = None
    IsClient2: Optional[bool] = None
    Score: Optional[int] = None
    IsNeglect: Optional[bool] = None
    IsRepair: Optional[bool] = None
    IsShared: Optional[bool] = None
    IsBid: Optional[bool] = None
    IsSce: Optional[bool] = None
    IsText: Optional[bool] = None
    IsAudio: Optional[bool] = None
    IsVideo: Optional[bool] = None
    Affect: Optional[int] = None
    IsSharedDate: Optional[datetime] = None
    EventDate: Optional[datetime] = None
    TherapistNotes: Optional[str] = None

class Event(EventBase):
    Id: int

    model_config = {"from_attributes": True}

# Statistics Models
class ClientStatistics(BaseModel):
    ClientId: int
    TotalEvents: int
    Client1Events: int
    Client2Events: int
    AverageScore: float
    NeglectCount: int
    RepairCount: int
    SharedCount: int
    BidCount: int
    TextCount: int
    AudioCount: int
    VideoCount: int

class TherapistStatistics(BaseModel):
    TherapistId: int
    TotalClients: int
    TotalEvents: int
    AverageScore: float

# Firebase Auth Models
class FirebaseTokenRequest(BaseModel):
    # Accept both "Email" (original) and "email" (mobile convention)
    model_config = ConfigDict(populate_by_name=True)
    Email: EmailStr = Field(alias="email")

class FirebaseTokenResponse(BaseModel):
    CustomToken: str
    UserID: int
    Email: str
    FullName: Optional[str] = None
    RoleID: Optional[int] = None
    RoleName: Optional[str] = None
    RedirectURL: Optional[str] = None

# ==================== ROOT ENDPOINT ====================

@app.get("/")
def read_root():
    """API root endpoint with available routes"""
    return {
        "message": "Welcome to UCANRR1 API",
        "version": "2.0.0",
        "documentation": "/docs",
        "cors_enabled": True,
        "features": ["Firebase Custom Auth", "Role-Based Authentication", "CRUD Operations", "Statistics"],
        "endpoints": {
            "auth": {
                "firebase_token": "POST /auth/firebase-token"
            },
            "roles": {
                "list": "GET /roles/",
                "create": "POST /roles/",
                "get": "GET /roles/{role_id}",
                "get_by_name": "GET /roles/name/{role_name}",
                "update": "PUT /roles/{role_id}",
                "delete": "DELETE /roles/{role_id}",
                "users": "GET /roles/{role_id}/users"
            },
            "authorized_users": {
                "list": "GET /authorized-users/",
                "list_with_roles": "GET /authorized-users/with-roles",
                "create": "POST /authorized-users/",
                "get": "GET /authorized-users/{user_id}",
                "get_with_role": "GET /authorized-users/{user_id}/with-role",
                "get_by_email": "GET /authorized-users/email/{email}",
                "update": "PUT /authorized-users/{user_id}",
                "update_login": "PUT /authorized-users/{user_id}/last-login",
                "delete": "DELETE /authorized-users/{user_id}"
            },
            "therapists": {
                "list": "GET /therapists/",
                "create": "POST /therapists/",
                "get": "GET /therapists/{therapist_id}",
                "update": "PUT /therapists/{therapist_id}",
                "delete": "DELETE /therapists/{therapist_id}",
                "clients": "GET /therapists/{therapist_id}/clients",
                "statistics": "GET /therapists/{therapist_id}/statistics"
            },
            "clients": {
                "list": "GET /clients/",
                "create": "POST /clients/",
                "get": "GET /clients/{client_id}",
                "update": "PUT /clients/{client_id}",
                "delete": "DELETE /clients/{client_id}",
                "events": "GET /clients/{client_id}/events",
                "statistics": "GET /clients/{client_id}/statistics"
            },
            "events": {
                "list": "GET /events/",
                "create": "POST /events/",
                "get": "GET /events/{event_id}",
                "update": "PUT /events/{event_id}",
                "delete": "DELETE /events/{event_id}"
            }
        }
    }

# ==================== FIREBASE CUSTOM AUTH ====================

@app.post("/auth/firebase-token", response_model=FirebaseTokenResponse)
def get_firebase_token(request: FirebaseTokenRequest):
    """
    Issue a Firebase Custom Token for an authorized email address.

    The client uses the returned CustomToken to sign in via the Firebase SDK
    (signInWithCustomToken). Only emails present in the AuthorizedUsers table
    with IsActive = true are granted a token. LastLogin is updated on success.
    """
    email = request.Email.lower()

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT au.UserID, au.Email, au.FullName, au.IsActive, au.RoleID, "
            "       r.RoleName, r.RedirectURL "
            "FROM AuthorizedUsers au "
            "LEFT JOIN Roles r ON au.RoleID = r.RoleID "
            "WHERE LOWER(au.Email) = ?",
            email
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=401, detail="Email not authorized")
        if not row.IsActive:
            raise HTTPException(status_code=403, detail="Account is inactive")

        # Create Firebase custom token using the email as the UID
        try:
            custom_token_bytes = firebase_auth.create_custom_token(email)
            custom_token = custom_token_bytes.decode("utf-8") if isinstance(custom_token_bytes, bytes) else custom_token_bytes
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to create Firebase token: {exc}")

        # Update LastLogin
        cursor.execute(
            "UPDATE AuthorizedUsers SET LastLogin = GETDATE() WHERE UserID = ?",
            row.UserID
        )
        conn.commit()

        return {
            "CustomToken": custom_token,
            "UserID": row.UserID,
            "Email": row.Email,
            "FullName": row.FullName,
            "RoleID": row.RoleID,
            "RoleName": row.RoleName,
            "RedirectURL": row.RedirectURL,
        }

# ==================== ROLES CRUD ====================

@app.post("/roles/", response_model=Role, status_code=status.HTTP_201_CREATED)
def create_role(role: RoleCreate):
    """Create a new role"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO Roles (RoleName, RedirectURL, Description, IsActive, CreatedAt) "
                "VALUES (?, ?, ?, ?, GETDATE())",
                role.RoleName, role.RedirectURL, role.Description, role.IsActive
            )
            conn.commit()
            cursor.execute("SELECT @@IDENTITY")
            new_id = cursor.fetchone()[0]
            cursor.execute(
                "SELECT RoleID, RoleName, RedirectURL, Description, IsActive, CreatedAt "
                "FROM Roles WHERE RoleID = ?", new_id
            )
            row = cursor.fetchone()
            return {
                "RoleID": row.RoleID,
                "RoleName": row.RoleName,
                "RedirectURL": row.RedirectURL,
                "Description": row.Description,
                "IsActive": row.IsActive,
                "CreatedAt": row.CreatedAt
            }
        except pyodbc.IntegrityError:
            raise HTTPException(status_code=400, detail="Role name already exists")

@app.get("/roles/", response_model=List[Role])
def read_roles(
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all roles with optional filtering and pagination"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        if is_active is not None:
            cursor.execute(
                "SELECT RoleID, RoleName, RedirectURL, Description, IsActive, CreatedAt "
                "FROM Roles WHERE IsActive = ? ORDER BY RoleID OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                is_active, skip, limit
            )
        else:
            cursor.execute(
                "SELECT RoleID, RoleName, RedirectURL, Description, IsActive, CreatedAt "
                "FROM Roles ORDER BY RoleID OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                skip, limit
            )

        rows = cursor.fetchall()
        return [
            {
                "RoleID": row.RoleID,
                "RoleName": row.RoleName,
                "RedirectURL": row.RedirectURL,
                "Description": row.Description,
                "IsActive": row.IsActive,
                "CreatedAt": row.CreatedAt
            }
            for row in rows
        ]

@app.get("/roles/{role_id}", response_model=Role)
def read_role(role_id: int):
    """Get a specific role by ID"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT RoleID, RoleName, RedirectURL, Description, IsActive, CreatedAt "
            "FROM Roles WHERE RoleID = ?",
            role_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Role not found")
        return {
            "RoleID": row.RoleID,
            "RoleName": row.RoleName,
            "RedirectURL": row.RedirectURL,
            "Description": row.Description,
            "IsActive": row.IsActive,
            "CreatedAt": row.CreatedAt
        }

@app.get("/roles/name/{role_name}", response_model=Role)
def read_role_by_name(role_name: str):
    """Get a specific role by name"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT RoleID, RoleName, RedirectURL, Description, IsActive, CreatedAt "
            "FROM Roles WHERE RoleName = ?",
            role_name
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Role not found")
        return {
            "RoleID": row.RoleID,
            "RoleName": row.RoleName,
            "RedirectURL": row.RedirectURL,
            "Description": row.Description,
            "IsActive": row.IsActive,
            "CreatedAt": row.CreatedAt
        }

@app.put("/roles/{role_id}", response_model=Role)
def update_role(role_id: int, role: RoleUpdate):
    """Update a role"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Check if exists
        cursor.execute("SELECT * FROM Roles WHERE RoleID = ?", role_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Role not found")

        # Build update query
        updates = []
        params = []

        if role.RoleName is not None:
            updates.append("RoleName = ?")
            params.append(role.RoleName)
        if role.RedirectURL is not None:
            updates.append("RedirectURL = ?")
            params.append(role.RedirectURL)
        if role.Description is not None:
            updates.append("Description = ?")
            params.append(role.Description)
        if role.IsActive is not None:
            updates.append("IsActive = ?")
            params.append(role.IsActive)

        if updates:
            params.append(role_id)
            try:
                cursor.execute(
                    f"UPDATE Roles SET {', '.join(updates)} WHERE RoleID = ?",
                    *params
                )
                conn.commit()
            except pyodbc.IntegrityError:
                raise HTTPException(status_code=400, detail="Role name already exists")

        return read_role(role_id)

@app.delete("/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_role(role_id: int):
    """Delete a role"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Check if role is in use
        cursor.execute("SELECT COUNT(*) FROM AuthorizedUsers WHERE RoleID = ?", role_id)
        count = cursor.fetchone()[0]
        if count > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete role: {count} user(s) are assigned to this role"
            )

        cursor.execute("DELETE FROM Roles WHERE RoleID = ?", role_id)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Role not found")
        conn.commit()

@app.get("/roles/{role_id}/users", response_model=List[AuthorizedUser])
def read_role_users(
    role_id: int,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all users assigned to a specific role"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Verify role exists
        cursor.execute("SELECT RoleID FROM Roles WHERE RoleID = ?", role_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Role not found")

        cursor.execute(
            "SELECT UserID, Email, FullName, IsActive, CreatedAt, LastLogin, RoleID "
            "FROM AuthorizedUsers WHERE RoleID = ? ORDER BY UserID OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            role_id, skip, limit
        )
        rows = cursor.fetchall()
        return [
            {
                "UserID": row.UserID,
                "Email": row.Email,
                "FullName": row.FullName,
                "IsActive": row.IsActive,
                "CreatedAt": row.CreatedAt,
                "LastLogin": row.LastLogin,
                "RoleID": row.RoleID
            }
            for row in rows
        ]

# ==================== AUTHORIZED USERS CRUD (UPDATED) ====================

@app.post("/authorized-users/", response_model=AuthorizedUser, status_code=status.HTTP_201_CREATED)
def create_authorized_user(user: AuthorizedUserCreate):
    """Create a new authorized user"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Verify RoleID exists if provided
        if user.RoleID is not None:
            cursor.execute("SELECT RoleID FROM Roles WHERE RoleID = ?", user.RoleID)
            if not cursor.fetchone():
                raise HTTPException(status_code=400, detail="Invalid RoleID: Role not found")

        try:
            cursor.execute(
                "INSERT INTO AuthorizedUsers (Email, FullName, IsActive, CreatedAt, RoleID) "
                "VALUES (?, ?, ?, GETDATE(), ?)",
                user.Email, user.FullName, user.IsActive, user.RoleID
            )
            conn.commit()
            cursor.execute("SELECT @@IDENTITY")
            new_id = cursor.fetchone()[0]
            cursor.execute(
                "SELECT UserID, Email, FullName, IsActive, CreatedAt, LastLogin, RoleID "
                "FROM AuthorizedUsers WHERE UserID = ?", new_id
            )
            row = cursor.fetchone()
            return {
                "UserID": row.UserID,
                "Email": row.Email,
                "FullName": row.FullName,
                "IsActive": row.IsActive,
                "CreatedAt": row.CreatedAt,
                "LastLogin": row.LastLogin,
                "RoleID": row.RoleID
            }
        except pyodbc.IntegrityError:
            raise HTTPException(status_code=400, detail="Email already exists")

@app.get("/authorized-users/", response_model=List[AuthorizedUser])
def read_authorized_users(
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    role_id: Optional[int] = Query(None, description="Filter by role ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all authorized users with optional filtering and pagination"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = "SELECT UserID, Email, FullName, IsActive, CreatedAt, LastLogin, RoleID FROM AuthorizedUsers WHERE 1=1"
        params = []

        if is_active is not None:
            query += " AND IsActive = ?"
            params.append(is_active)

        if role_id is not None:
            query += " AND RoleID = ?"
            params.append(role_id)

        query += " ORDER BY UserID OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([skip, limit])

        cursor.execute(query, *params)
        rows = cursor.fetchall()
        return [
            {
                "UserID": row.UserID,
                "Email": row.Email,
                "FullName": row.FullName,
                "IsActive": row.IsActive,
                "CreatedAt": row.CreatedAt,
                "LastLogin": row.LastLogin,
                "RoleID": row.RoleID
            }
            for row in rows
        ]

@app.get("/authorized-users/with-roles", response_model=List[AuthorizedUserWithRole])
def read_authorized_users_with_roles(
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    role_id: Optional[int] = Query(None, description="Filter by role ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all authorized users with their role details"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT u.UserID, u.Email, u.FullName, u.IsActive, u.CreatedAt, u.LastLogin,
                   u.RoleID, r.RoleName, r.RedirectURL
            FROM AuthorizedUsers u
            LEFT JOIN Roles r ON u.RoleID = r.RoleID
            WHERE 1=1
        """
        params = []

        if is_active is not None:
            query += " AND u.IsActive = ?"
            params.append(is_active)

        if role_id is not None:
            query += " AND u.RoleID = ?"
            params.append(role_id)

        query += " ORDER BY u.UserID OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([skip, limit])

        cursor.execute(query, *params)
        rows = cursor.fetchall()
        return [
            {
                "UserID": row[0],
                "Email": row[1],
                "FullName": row[2],
                "IsActive": row[3],
                "CreatedAt": row[4],
                "LastLogin": row[5],
                "RoleID": row[6],
                "RoleName": row[7],
                "RedirectURL": row[8]
            }
            for row in rows
        ]

@app.get("/authorized-users/{user_id}", response_model=AuthorizedUser)
def read_authorized_user(user_id: int):
    """Get a specific authorized user by ID"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT UserID, Email, FullName, IsActive, CreatedAt, LastLogin, RoleID "
            "FROM AuthorizedUsers WHERE UserID = ?",
            user_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Authorized user not found")
        return {
            "UserID": row.UserID,
            "Email": row.Email,
            "FullName": row.FullName,
            "IsActive": row.IsActive,
            "CreatedAt": row.CreatedAt,
            "LastLogin": row.LastLogin,
            "RoleID": row.RoleID
        }

@app.get("/authorized-users/{user_id}/with-role", response_model=AuthorizedUserWithRole)
def read_authorized_user_with_role(user_id: int):
    """Get a specific authorized user with role details"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT u.UserID, u.Email, u.FullName, u.IsActive, u.CreatedAt, u.LastLogin,
                   u.RoleID, r.RoleName, r.RedirectURL
            FROM AuthorizedUsers u
            LEFT JOIN Roles r ON u.RoleID = r.RoleID
            WHERE u.UserID = ?
            """,
            user_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Authorized user not found")
        return {
            "UserID": row[0],
            "Email": row[1],
            "FullName": row[2],
            "IsActive": row[3],
            "CreatedAt": row[4],
            "LastLogin": row[5],
            "RoleID": row[6],
            "RoleName": row[7],
            "RedirectURL": row[8]
        }

@app.get("/authorized-users/email/{email}", response_model=AuthorizedUser)
def read_authorized_user_by_email(email: str):
    """Get a specific authorized user by email"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT UserID, Email, FullName, IsActive, CreatedAt, LastLogin, RoleID "
            "FROM AuthorizedUsers WHERE Email = ?",
            email
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Authorized user not found")
        return {
            "UserID": row.UserID,
            "Email": row.Email,
            "FullName": row.FullName,
            "IsActive": row.IsActive,
            "CreatedAt": row.CreatedAt,
            "LastLogin": row.LastLogin,
            "RoleID": row.RoleID
        }

@app.put("/authorized-users/{user_id}", response_model=AuthorizedUser)
def update_authorized_user(user_id: int, user: AuthorizedUserUpdate):
    """Update an authorized user"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Check if exists
        cursor.execute("SELECT * FROM AuthorizedUsers WHERE UserID = ?", user_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Authorized user not found")

        # Verify RoleID exists if provided
        if user.RoleID is not None:
            cursor.execute("SELECT RoleID FROM Roles WHERE RoleID = ?", user.RoleID)
            if not cursor.fetchone():
                raise HTTPException(status_code=400, detail="Invalid RoleID: Role not found")

        # Build update query
        updates = []
        params = []

        if user.Email is not None:
            updates.append("Email = ?")
            params.append(user.Email)
        if user.FullName is not None:
            updates.append("FullName = ?")
            params.append(user.FullName)
        if user.IsActive is not None:
            updates.append("IsActive = ?")
            params.append(user.IsActive)
        if user.LastLogin is not None:
            updates.append("LastLogin = ?")
            params.append(user.LastLogin)
        if user.RoleID is not None:
            updates.append("RoleID = ?")
            params.append(user.RoleID)

        if updates:
            params.append(user_id)
            try:
                cursor.execute(
                    f"UPDATE AuthorizedUsers SET {', '.join(updates)} WHERE UserID = ?",
                    *params
                )
                conn.commit()
            except pyodbc.IntegrityError:
                raise HTTPException(status_code=400, detail="Email already exists")

        return read_authorized_user(user_id)

@app.put("/authorized-users/{user_id}/last-login", response_model=AuthorizedUser)
def update_last_login(user_id: int):
    """Update the last login timestamp for an authorized user"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Check if exists
        cursor.execute("SELECT * FROM AuthorizedUsers WHERE UserID = ?", user_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Authorized user not found")

        cursor.execute(
            "UPDATE AuthorizedUsers SET LastLogin = GETDATE() WHERE UserID = ?",
            user_id
        )
        conn.commit()

        return read_authorized_user(user_id)

@app.delete("/authorized-users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_authorized_user(user_id: int):
    """Delete an authorized user"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM AuthorizedUsers WHERE UserID = ?", user_id)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Authorized user not found")
        conn.commit()

# ==================== THERAPIST CRUD ====================

@app.post("/therapists/", response_model=Therapist, status_code=status.HTTP_201_CREATED)
def create_therapist(therapist: TherapistCreate):
    """Create a new therapist"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO Therapist (TherapistUserName, ThearapistPassword, ClientId, CreatedDate, UpdatedDate) "
            "VALUES (?, ?, ?, GETDATE(), GETDATE())",
            therapist.TherapistUserName, therapist.ThearapistPassword, therapist.ClientId
        )
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        new_id = cursor.fetchone()[0]
        cursor.execute(
            "SELECT Id, TherapistUserName, ThearapistPassword, ClientId, CreatedDate, UpdatedDate "
            "FROM Therapist WHERE Id = ?", new_id
        )
        row = cursor.fetchone()
        return {
            "Id": row.Id,
            "TherapistUserName": row.TherapistUserName.strip(),
            "ThearapistPassword": row.ThearapistPassword.strip(),
            "ClientId": row.ClientId,
            "CreatedDate": row.CreatedDate,
            "UpdatedDate": row.UpdatedDate
        }

@app.get("/therapists/", response_model=List[Therapist])
def read_therapists(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all therapists with pagination"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Id, TherapistUserName, ThearapistPassword, ClientId, CreatedDate, UpdatedDate "
            "FROM Therapist ORDER BY Id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            skip, limit
        )
        rows = cursor.fetchall()
        return [
            {
                "Id": row.Id,
                "TherapistUserName": row.TherapistUserName.strip(),
                "ThearapistPassword": row.ThearapistPassword.strip(),
                "ClientId": row.ClientId,
                "CreatedDate": row.CreatedDate,
                "UpdatedDate": row.UpdatedDate
            }
            for row in rows
        ]

@app.get("/therapists/{therapist_id}", response_model=Therapist)
def read_therapist(therapist_id: int):
    """Get a specific therapist by ID"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Id, TherapistUserName, ThearapistPassword, ClientId, CreatedDate, UpdatedDate "
            "FROM Therapist WHERE Id = ?",
            therapist_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Therapist not found")
        return {
            "Id": row.Id,
            "TherapistUserName": row.TherapistUserName.strip(),
            "ThearapistPassword": row.ThearapistPassword.strip(),
            "ClientId": row.ClientId,
            "CreatedDate": row.CreatedDate,
            "UpdatedDate": row.UpdatedDate
        }

@app.put("/therapists/{therapist_id}", response_model=Therapist)
def update_therapist(therapist_id: int, therapist: TherapistUpdate):
    """Update a therapist"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Check if exists
        cursor.execute("SELECT * FROM Therapist WHERE Id = ?", therapist_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Therapist not found")

        # Build update query
        updates = ["UpdatedDate = GETDATE()"]
        params = []
        if therapist.TherapistUserName is not None:
            updates.append("TherapistUserName = ?")
            params.append(therapist.TherapistUserName)
        if therapist.ThearapistPassword is not None:
            updates.append("ThearapistPassword = ?")
            params.append(therapist.ThearapistPassword)
        if therapist.ClientId is not None:
            updates.append("ClientId = ?")
            params.append(therapist.ClientId)

        params.append(therapist_id)
        cursor.execute(
            f"UPDATE Therapist SET {', '.join(updates)} WHERE Id = ?",
            *params
        )
        conn.commit()

        return read_therapist(therapist_id)

@app.delete("/therapists/{therapist_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_therapist(therapist_id: int):
    """Delete a therapist"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Therapist WHERE Id = ?", therapist_id)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Therapist not found")
        conn.commit()

# ==================== CLIENT CRUD ====================

@app.post("/clients/", response_model=Client, status_code=status.HTTP_201_CREATED)
def create_client(client: ClientCreate):
    """Create a new client"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO Client (TherapistID, Client1FirstName, Client1UserName, Client1Phone, "
            "Client2FirstName, Client2UserName, Client2Phone, CreatedDate, UpdatedDate) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())",
            client.TherapistID, client.Client1FirstName, client.Client1UserName,
            client.Client1Phone, client.Client2FirstName, client.Client2UserName, client.Client2Phone
        )
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        new_id = cursor.fetchone()[0]
        cursor.execute(
            "SELECT Id, TherapistID, Client1FirstName, Client1UserName, Client1Phone, "
            "Client2FirstName, Client2UserName, Client2Phone, CreatedDate, UpdatedDate FROM Client WHERE Id = ?", new_id
        )
        row = cursor.fetchone()
        return {
            "Id": row.Id,
            "TherapistID": row.TherapistID,
            "Client1FirstName": row.Client1FirstName.strip() if row.Client1FirstName else None,
            "Client1UserName": row.Client1UserName.strip(),
            "Client1Phone": row.Client1Phone.strip(),
            "Client2FirstName": row.Client2FirstName.strip() if row.Client2FirstName else None,
            "Client2UserName": row.Client2UserName.strip(),
            "Client2Phone": row.Client2Phone.strip(),
            "CreatedDate": row.CreatedDate,
            "UpdatedDate": row.UpdatedDate
        }

@app.get("/clients/", response_model=List[Client])
def read_clients(
    therapist_id: Optional[int] = Query(None, description="Filter by therapist ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all clients with optional filtering and pagination"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        if therapist_id is not None:
            cursor.execute(
                "SELECT Id, TherapistID, Client1FirstName, Client1UserName, Client1Phone, "
                "Client2FirstName, Client2UserName, Client2Phone, CreatedDate, UpdatedDate "
                "FROM Client WHERE TherapistID = ? ORDER BY Id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                therapist_id, skip, limit
            )
        else:
            cursor.execute(
                "SELECT Id, TherapistID, Client1FirstName, Client1UserName, Client1Phone, "
                "Client2FirstName, Client2UserName, Client2Phone, CreatedDate, UpdatedDate "
                "FROM Client ORDER BY Id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                skip, limit
            )

        rows = cursor.fetchall()
        return [
            {
                "Id": row.Id,
                "TherapistID": row.TherapistID,
                "Client1FirstName": row.Client1FirstName.strip() if row.Client1FirstName else None,
                "Client1UserName": row.Client1UserName.strip(),
                "Client1Phone": row.Client1Phone.strip(),
                "Client2FirstName": row.Client2FirstName.strip() if row.Client2FirstName else None,
                "Client2UserName": row.Client2UserName.strip(),
                "Client2Phone": row.Client2Phone.strip(),
                "CreatedDate": row.CreatedDate,
                "UpdatedDate": row.UpdatedDate
            }
            for row in rows
        ]

@app.get("/clients/{client_id}", response_model=Client)
def read_client(client_id: int):
    """Get a specific client by ID"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Id, TherapistID, Client1FirstName, Client1UserName, Client1Phone, "
            "Client2FirstName, Client2UserName, Client2Phone, CreatedDate, UpdatedDate FROM Client WHERE Id = ?",
            client_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Client not found")
        return {
            "Id": row.Id,
            "TherapistID": row.TherapistID,
            "Client1FirstName": row.Client1FirstName.strip() if row.Client1FirstName else None,
            "Client1UserName": row.Client1UserName.strip(),
            "Client1Phone": row.Client1Phone.strip(),
            "Client2FirstName": row.Client2FirstName.strip() if row.Client2FirstName else None,
            "Client2UserName": row.Client2UserName.strip(),
            "Client2Phone": row.Client2Phone.strip(),
            "CreatedDate": row.CreatedDate,
            "UpdatedDate": row.UpdatedDate
        }

@app.put("/clients/{client_id}", response_model=Client)
def update_client(client_id: int, client: ClientUpdate):
    """Update a client"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM Client WHERE Id = ?", client_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Client not found")

        updates = ["UpdatedDate = GETDATE()"]
        params = []
        for field, value in client.model_dump(exclude_unset=True).items():
            if value is not None:
                updates.append(f"{field} = ?")
                params.append(value)

        if len(params) > 0:
            params.append(client_id)
            cursor.execute(
                f"UPDATE Client SET {', '.join(updates)} WHERE Id = ?",
                *params
            )
            conn.commit()

        return read_client(client_id)

@app.delete("/clients/{client_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_client(client_id: int):
    """Delete a client"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Client WHERE Id = ?", client_id)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Client not found")
        conn.commit()

# ==================== EVENT CRUD ====================

@app.post("/events/", response_model=Event, status_code=status.HTTP_201_CREATED)
def create_event(event: EventCreate):
    """Create a new event"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO Event (ClientId, IsClient1, IsClient2, Score, IsNeglect, IsRepair, IsShared, IsBid, IsSce, IsText, "
            "IsAudio, IsVideo, Affect, IsSharedDate, EventDate, TherapistNotes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            event.ClientId, event.IsClient1, event.IsClient2, event.Score, event.IsNeglect, event.IsRepair, event.IsShared,
            event.IsBid, event.IsSce, event.IsText, event.IsAudio, event.IsVideo,
            event.Affect, event.IsSharedDate, event.EventDate, event.TherapistNotes
        )
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        new_id = cursor.fetchone()[0]
        return {**event.model_dump(), "Id": new_id}

@app.get("/events/", response_model=List[Event])
def read_events(
    client_id: Optional[int] = Query(None, description="Filter by client ID"),
    start_date: Optional[datetime] = Query(None, description="Filter events from this date"),
    end_date: Optional[datetime] = Query(None, description="Filter events until this date"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all events with optional filtering and pagination"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT Id, ClientId, IsClient1, IsClient2, Score, IsNeglect, IsRepair, IsShared, IsBid, IsSce, IsText,
            IsAudio, IsVideo, Affect, IsSharedDate, EventDate, TherapistNotes
            FROM Event WHERE 1=1
        """
        params = []

        if client_id is not None:
            query += " AND ClientId = ?"
            params.append(client_id)

        if start_date is not None:
            query += " AND EventDate >= ?"
            params.append(start_date)

        if end_date is not None:
            query += " AND EventDate <= ?"
            params.append(end_date)

        query += " ORDER BY EventDate DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([skip, limit])

        cursor.execute(query, *params)
        rows = cursor.fetchall()
        return [
            {
                "Id": row.Id,
                "ClientId": row.ClientId,
                "IsClient1": row.IsClient1,
                "IsClient2": row.IsClient2,
                "Score": row.Score,
                "IsNeglect": row.IsNeglect,
                "IsRepair": row.IsRepair,
                "IsShared": row.IsShared,
                "IsBid": row.IsBid,
                "IsSce": row.IsSce,
                "IsText": row.IsText,
                "IsAudio": row.IsAudio,
                "IsVideo": row.IsVideo,
                "Affect": row.Affect,
                "IsSharedDate": row.IsSharedDate,
                "EventDate": row.EventDate,
                "TherapistNotes": row.TherapistNotes
            }
            for row in rows
        ]

@app.get("/events/{event_id}", response_model=Event)
def read_event(event_id: int):
    """Get a specific event by ID"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Id, ClientId, IsClient1, IsClient2, Score, IsNeglect, IsRepair, IsShared, IsBid, IsSce, IsText, "
            "IsAudio, IsVideo, Affect, IsSharedDate, EventDate, TherapistNotes "
            "FROM Event WHERE Id = ?",
            event_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Event not found")
        return {
            "Id": row.Id,
            "ClientId": row.ClientId,
            "IsClient1": row.IsClient1,
            "IsClient2": row.IsClient2,
            "Score": row.Score,
            "IsNeglect": row.IsNeglect,
            "IsRepair": row.IsRepair,
            "IsShared": row.IsShared,
            "IsBid": row.IsBid,
            "IsText": row.IsText,
            "IsAudio": row.IsAudio,
            "IsVideo": row.IsVideo,
            "Affect": row.Affect,
            "IsSharedDate": row.IsSharedDate,
            "EventDate": row.EventDate,
            "TherapistNotes": row.TherapistNotes
        }

@app.put("/events/{event_id}", response_model=Event)
def update_event(event_id: int, event: EventUpdate):
    """Update an event"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM Event WHERE Id = ?", event_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Event not found")

        updates = []
        params = []
        for field, value in event.model_dump(exclude_unset=True).items():
            updates.append(f"{field} = ?")
            params.append(value)

        if updates:
            params.append(event_id)
            cursor.execute(
                f"UPDATE Event SET {', '.join(updates)} WHERE Id = ?",
                *params
            )
            conn.commit()

        return read_event(event_id)

@app.delete("/events/{event_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_event(event_id: int):
    """Delete an event"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Event WHERE Id = ?", event_id)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Event not found")
        conn.commit()

# ==================== RELATIONSHIP ENDPOINTS ====================

@app.get("/therapists/{therapist_id}/clients", response_model=List[Client])
def read_therapist_clients(
    therapist_id: int,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all clients for a specific therapist"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Verify therapist exists
        cursor.execute("SELECT Id FROM Therapist WHERE Id = ?", therapist_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Therapist not found")

        cursor.execute(
            "SELECT Id, TherapistID, Client1FirstName, Client1UserName, Client1Phone, "
            "Client2FirstName, Client2UserName, Client2Phone, CreatedDate, UpdatedDate "
            "FROM Client WHERE TherapistID = ? ORDER BY Id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            therapist_id, skip, limit
        )
        rows = cursor.fetchall()
        return [
            {
                "Id": row.Id,
                "TherapistID": row.TherapistID,
                "Client1FirstName": row.Client1FirstName.strip() if row.Client1FirstName else None,
                "Client1UserName": row.Client1UserName.strip(),
                "Client1Phone": row.Client1Phone.strip(),
                "Client2FirstName": row.Client2FirstName.strip() if row.Client2FirstName else None,
                "Client2UserName": row.Client2UserName.strip(),
                "Client2Phone": row.Client2Phone.strip(),
                "CreatedDate": row.CreatedDate,
                "UpdatedDate": row.UpdatedDate
            }
            for row in rows
        ]

@app.get("/clients/{client_id}/events", response_model=List[Event])
def read_client_events(
    client_id: int,
    start_date: Optional[datetime] = Query(None, description="Filter events from this date"),
    end_date: Optional[datetime] = Query(None, description="Filter events until this date"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """Get all events for a specific client"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Verify client exists
        cursor.execute("SELECT Id FROM Client WHERE Id = ?", client_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Client not found")

        query = """
            SELECT Id, ClientId, IsClient1, IsClient2, Score, IsNeglect, IsRepair, IsShared, IsBid, IsSce, IsText,
            IsAudio, IsVideo, Affect, IsSharedDate, EventDate, TherapistNotes
            FROM Event WHERE ClientId = ?
        """
        params = [client_id]

        if start_date is not None:
            query += " AND EventDate >= ?"
            params.append(start_date)

        if end_date is not None:
            query += " AND EventDate <= ?"
            params.append(end_date)

        query += " ORDER BY EventDate DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([skip, limit])

        cursor.execute(query, *params)
        rows = cursor.fetchall()
        return [
            {
                "Id": row.Id,
                "ClientId": row.ClientId,
                "IsClient1": row.IsClient1,
                "IsClient2": row.IsClient2,
                "Score": row.Score,
                "IsNeglect": row.IsNeglect,
                "IsRepair": row.IsRepair,
                "IsShared": row.IsShared,
                "IsBid": row.IsBid,
                "IsSce": row.IsSce,
                "IsText": row.IsText,
                "IsAudio": row.IsAudio,
                "IsVideo": row.IsVideo,
                "Affect": row.Affect,
                "IsSharedDate": row.IsSharedDate,
                "EventDate": row.EventDate,
                "TherapistNotes": row.TherapistNotes
            }
            for row in rows
        ]

# ==================== STATISTICS ENDPOINTS ====================

@app.get("/clients/{client_id}/statistics", response_model=ClientStatistics)
def get_client_statistics(client_id: int):
    """Get statistics for a specific client"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Verify client exists
        cursor.execute("SELECT Id FROM Client WHERE Id = ?", client_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Client not found")

        cursor.execute("""
            SELECT
                COUNT(*) as TotalEvents,
                SUM(CASE WHEN IsClient1 = 1 THEN 1 ELSE 0 END) as Client1Events,
                SUM(CASE WHEN IsClient2 = 1 THEN 1 ELSE 0 END) as Client2Events,
                AVG(CAST(Score AS FLOAT)) as AverageScore,
                SUM(CASE WHEN IsNeglect = 1 THEN 1 ELSE 0 END) as NeglectCount,
                SUM(CASE WHEN IsRepair = 1 THEN 1 ELSE 0 END) as RepairCount,
                SUM(CASE WHEN IsShared = 1 THEN 1 ELSE 0 END) as SharedCount,
                SUM(CASE WHEN IsBid = 1 THEN 1 ELSE 0 END) as BidCount,
                SUM(CASE WHEN IsText = 1 THEN 1 ELSE 0 END) as TextCount,
                SUM(CASE WHEN IsAudio = 1 THEN 1 ELSE 0 END) as AudioCount,
                SUM(CASE WHEN IsVideo = 1 THEN 1 ELSE 0 END) as VideoCount
            FROM Event
            WHERE ClientId = ?
        """, client_id)

        row = cursor.fetchone()
        return {
            "ClientId": client_id,
            "TotalEvents": row[0] or 0,
            "Client1Events": row[1] or 0,
            "Client2Events": row[2] or 0,
            "AverageScore": round(row[3], 2) if row[3] else 0.0,
            "NeglectCount": row[4] or 0,
            "RepairCount": row[5] or 0,
            "SharedCount": row[6] or 0,
            "BidCount": row[7] or 0,
            "TextCount": row[8] or 0,
            "AudioCount": row[9] or 0,
            "VideoCount": row[10] or 0
        }

@app.get("/therapists/{therapist_id}/statistics", response_model=TherapistStatistics)
def get_therapist_statistics(therapist_id: int):
    """Get statistics for a specific therapist"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Verify therapist exists
        cursor.execute("SELECT Id FROM Therapist WHERE Id = ?", therapist_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Therapist not found")

        # Get client count
        cursor.execute("SELECT COUNT(*) FROM Client WHERE TherapistID = ?", therapist_id)
        total_clients = cursor.fetchone()[0]

        # Get event statistics
        cursor.execute("""
            SELECT
                COUNT(*) as TotalEvents,
                AVG(CAST(Score AS FLOAT)) as AverageScore
            FROM Event e
            INNER JOIN Client c ON e.ClientId = c.Id
            WHERE c.TherapistID = ?
        """, therapist_id)

        row = cursor.fetchone()
        return {
            "TherapistId": therapist_id,
            "TotalClients": total_clients or 0,
            "TotalEvents": row[0] or 0,
            "AverageScore": round(row[1], 2) if row[1] else 0.0
        }
