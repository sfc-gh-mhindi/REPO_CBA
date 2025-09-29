# Snowflake Streamlit App Roles and Privileges

## Overview

This document explains the role-based access control for Snowflake Streamlit applications, focusing on the distinction between OWNER and READ roles, and the specific privileges required for different Streamlit operations.

## Role Distinctions

### OWNER Role

The **OWNER** role provides full administrative control over a Streamlit application.

| **Capability** | **Description** |
|----------------|-----------------|
| **Full Control** | Complete administrative access to the Streamlit app |
| **Modify Code** | Can edit, update, and deploy new versions of the app |
| **Manage Access** | Can grant and revoke permissions to other users/roles |
| **Delete App** | Can permanently delete the Streamlit application |
| **View Logs** | Access to application logs and debugging information |
| **Configuration** | Can modify app settings, environment variables, and configurations |
| **Deployment** | Can deploy, redeploy, and manage app versions |

### READ Role

The **READ** role provides view-only access to a Streamlit application.

| **Capability** | **Description** |
|----------------|-----------------|
| **View Only** | Can access and interact with the Streamlit app interface |
| **Execute Functions** | Can use app functionality (buttons, inputs, queries) |
| **No Modifications** | Cannot edit code, settings, or configurations |
| **No Admin Access** | Cannot manage permissions or delete the app |
| **Limited Visibility** | No access to source code or administrative features |

## Key Differences Summary

| **Aspect** | **OWNER Role** | **READ Role** |
|------------|----------------|---------------|
| **App Access** | ✅ Full Access | ✅ View/Use Only |
| **Code Editing** | ✅ Yes | ❌ No |
| **Permission Management** | ✅ Yes | ❌ No |
| **App Deletion** | ✅ Yes | ❌ No |
| **Configuration Changes** | ✅ Yes | ❌ No |
| **Log Access** | ✅ Yes | ❌ No |
| **Deployment Control** | ✅ Yes | ❌ No |

---

## Required Privileges for Streamlit Operations

### 1. Creating a Streamlit App

To create a new Streamlit application, a user/role must have the following privileges:

#### Database and Schema Privileges
```sql
-- Database level privileges
GRANT USAGE ON DATABASE <database_name> TO ROLE <role_name>;
GRANT CREATE STREAMLIT ON DATABASE <database_name> TO ROLE <role_name>;

-- Schema level privileges  
GRANT USAGE ON SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;
GRANT CREATE STREAMLIT ON SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;
```

#### Warehouse Privileges
```sql
-- Warehouse access for app execution
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;
GRANT OPERATE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;
```

#### Additional Requirements
- **Role Assignment:** User must be assigned a role with the above privileges
- **Stage Access:** If using external files, appropriate stage privileges are required
- **Compute Resources:** Access to a warehouse for app execution

#### Example: Complete Setup for App Creation
```sql
-- Create a role for Streamlit developers
CREATE ROLE STREAMLIT_DEVELOPER;

-- Grant database and schema privileges
GRANT USAGE ON DATABASE ANALYTICS TO ROLE STREAMLIT_DEVELOPER;
GRANT CREATE STREAMLIT ON DATABASE ANALYTICS TO ROLE STREAMLIT_DEVELOPER;
GRANT USAGE ON SCHEMA ANALYTICS.APPS TO ROLE STREAMLIT_DEVELOPER;
GRANT CREATE STREAMLIT ON SCHEMA ANALYTICS.APPS TO ROLE STREAMLIT_DEVELOPER;

-- Grant warehouse privileges
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAMLIT_DEVELOPER;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE STREAMLIT_DEVELOPER;

-- Assign role to user
GRANT ROLE STREAMLIT_DEVELOPER TO USER john_doe;
```

### 2. Editing a Streamlit App

To edit an existing Streamlit application, a user/role needs:

#### Ownership or Explicit Privileges
```sql
-- Option A: Transfer ownership
GRANT OWNERSHIP ON STREAMLIT <database_name>.<schema_name>.<app_name> 
TO ROLE <role_name> REVOKE CURRENT GRANTS;

-- Option B: Grant modify privileges (if supported)
GRANT MODIFY ON STREAMLIT <database_name>.<schema_name>.<app_name> 
TO ROLE <role_name>;
```

#### Underlying Resource Access
```sql
-- Database and schema access
GRANT USAGE ON DATABASE <database_name> TO ROLE <role_name>;
GRANT USAGE ON SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;

-- Warehouse access for testing changes
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;
GRANT OPERATE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;
```

#### Data Access (if app queries data)
```sql
-- Table/view privileges for data access
GRANT SELECT ON TABLE <database_name>.<schema_name>.<table_name> TO ROLE <role_name>;
GRANT SELECT ON ALL TABLES IN SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;

-- Future grants for new tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;
```

#### Example: Setup for App Editing
```sql
-- Create role for app editors
CREATE ROLE STREAMLIT_EDITOR;

-- Grant ownership of specific app
GRANT OWNERSHIP ON STREAMLIT ANALYTICS.APPS.SALES_DASHBOARD 
TO ROLE STREAMLIT_EDITOR REVOKE CURRENT GRANTS;

-- Grant necessary resource access
GRANT USAGE ON DATABASE ANALYTICS TO ROLE STREAMLIT_EDITOR;
GRANT USAGE ON SCHEMA ANALYTICS.APPS TO ROLE STREAMLIT_EDITOR;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAMLIT_EDITOR;

-- Grant data access
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.SALES TO ROLE STREAMLIT_EDITOR;

-- Assign to user
GRANT ROLE STREAMLIT_EDITOR TO USER jane_smith;
```

### 3. Viewing a Streamlit App

To view and use a Streamlit application, a user/role needs minimal privileges:

#### App Access Privileges
```sql
-- Basic app access
GRANT USAGE ON STREAMLIT <database_name>.<schema_name>.<app_name> 
TO ROLE <role_name>;
```

#### Database and Schema Access
```sql
-- Database access
GRANT USAGE ON DATABASE <database_name> TO ROLE <role_name>;

-- Schema access
GRANT USAGE ON SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;
```

#### Warehouse Access (for app execution)
```sql
-- Warehouse usage for running the app
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;
```

#### Data Access (if app displays data)
```sql
-- Read access to underlying data
GRANT SELECT ON TABLE <database_name>.<schema_name>.<table_name> TO ROLE <role_name>;

-- Or schema-level access
GRANT SELECT ON ALL TABLES IN SCHEMA <database_name>.<schema_name> TO ROLE <role_name>;
```

#### Example: Setup for App Viewing
```sql
-- Create role for app viewers
CREATE ROLE STREAMLIT_VIEWER;

-- Grant app access
GRANT USAGE ON STREAMLIT ANALYTICS.APPS.SALES_DASHBOARD TO ROLE STREAMLIT_VIEWER;

-- Grant resource access
GRANT USAGE ON DATABASE ANALYTICS TO ROLE STREAMLIT_VIEWER;
GRANT USAGE ON SCHEMA ANALYTICS.APPS TO ROLE STREAMLIT_VIEWER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAMLIT_VIEWER;

-- Grant data access (read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.SALES TO ROLE STREAMLIT_VIEWER;

-- Assign to users
GRANT ROLE STREAMLIT_VIEWER TO USER viewer1;
GRANT ROLE STREAMLIT_VIEWER TO USER viewer2;
```

---

## Privilege Hierarchy and Best Practices

### Recommended Role Structure

```sql
-- 1. Streamlit Admin Role (Full Control)
CREATE ROLE STREAMLIT_ADMIN;
GRANT CREATE STREAMLIT ON DATABASE ANALYTICS TO ROLE STREAMLIT_ADMIN;
GRANT USAGE ON ALL WAREHOUSES TO ROLE STREAMLIT_ADMIN;

-- 2. Streamlit Developer Role (Create & Edit)
CREATE ROLE STREAMLIT_DEVELOPER;
GRANT CREATE STREAMLIT ON SCHEMA ANALYTICS.APPS TO ROLE STREAMLIT_DEVELOPER;
GRANT USAGE ON WAREHOUSE DEV_WH TO ROLE STREAMLIT_DEVELOPER;

-- 3. Streamlit Editor Role (Edit Specific Apps)
CREATE ROLE STREAMLIT_EDITOR;
-- Grant ownership on specific apps as needed

-- 4. Streamlit Viewer Role (Read-Only Access)
CREATE ROLE STREAMLIT_VIEWER;
GRANT USAGE ON WAREHOUSE VIEWER_WH TO ROLE STREAMLIT_VIEWER;
```

### Security Best Practices

#### Principle of Least Privilege
- Grant only the minimum privileges required for each role
- Use specific object grants rather than broad schema-level grants when possible
- Regularly audit and review granted privileges

#### Role Separation
- **Separate development and production environments**
- **Use different roles for different environments**
- **Limit production access to authorized personnel only**

## Common Scenarios and Solutions

### Scenario 1: User Cannot Create Streamlit App

**Problem:** User gets "Insufficient privileges" error when creating app

**Solution:**
```sql
-- Check current privileges
SHOW GRANTS TO USER <username>;

-- Grant required privileges
GRANT USAGE ON DATABASE <db_name> TO ROLE <user_role>;
GRANT CREATE STREAMLIT ON SCHEMA <db_name>.<schema_name> TO ROLE <user_role>;
GRANT USAGE ON WAREHOUSE <wh_name> TO ROLE <user_role>;
```

### Scenario 2: User Can View But Cannot Edit App

**Problem:** User can access app but cannot modify code

**Solution:**
```sql
-- Transfer ownership or grant modify privileges
GRANT OWNERSHIP ON STREAMLIT <db_name>.<schema_name>.<app_name> 
TO ROLE <user_role> REVOKE CURRENT GRANTS;
```

### Scenario 3: App Runs But Cannot Access Data

**Problem:** Streamlit app loads but shows "no data" or permission errors

**Solution:**
```sql
-- Grant data access privileges
GRANT SELECT ON ALL TABLES IN SCHEMA <data_schema> TO ROLE <app_role>;
GRANT SELECT ON ALL VIEWS IN SCHEMA <data_schema> TO ROLE <app_role>;

-- For future objects
GRANT SELECT ON FUTURE TABLES IN SCHEMA <data_schema> TO ROLE <app_role>;
```

### Scenario 4: Performance Issues with Streamlit Apps

**Problem:** Apps run slowly or time out

**Solution:**
```sql
-- Ensure adequate warehouse resources
ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = 'MEDIUM';

-- Grant OPERATE privilege for auto-resume
GRANT OPERATE ON WAREHOUSE <warehouse_name> TO ROLE <app_role>;

-- Consider dedicated warehouse for Streamlit apps
CREATE WAREHOUSE STREAMLIT_WH WITH 
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```

---

## Troubleshooting Guide

### Common Error Messages

| **Error Message** | **Cause** | **Solution** |
|-------------------|-----------|--------------|
| "Insufficient privileges to operate on STREAMLIT" | Missing CREATE STREAMLIT privilege | Grant CREATE STREAMLIT on database/schema |
| "Object does not exist or not authorized" | Missing USAGE privilege on app | Grant USAGE on specific Streamlit app |
| "Warehouse not found or not authorized" | Missing warehouse privileges | Grant USAGE and OPERATE on warehouse |
| "SQL compilation error: Object not found" | Missing data access privileges | Grant SELECT on required tables/views |

### Diagnostic Queries

```sql
-- Check user's current role and privileges
SELECT CURRENT_ROLE(), CURRENT_USER();
SHOW GRANTS TO ROLE <role_name>;

-- Check Streamlit app permissions
SHOW GRANTS ON STREAMLIT <database>.<schema>.<app_name>;

-- Check warehouse access
SHOW GRANTS ON WAREHOUSE <warehouse_name>;

-- Verify database and schema access
SHOW GRANTS ON DATABASE <database_name>;
SHOW GRANTS ON SCHEMA <database_name>.<schema_name>;
```

---

## Additional Resources

### Snowflake Documentation
- [Streamlit in Snowflake Documentation](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Access Control Privileges](https://docs.snowflake.com/en/user-guide/security-access-control-privileges)
- [Role-Based Access Control](https://docs.snowflake.com/en/user-guide/security-access-control-overview)

### Best Practice Guidelines
- Always test privilege grants in a development environment first
- Document role assignments and privilege grants for audit purposes
- Regularly review and clean up unused roles and privileges
- Use resource monitors to control warehouse usage for Streamlit apps

---