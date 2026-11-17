Week 1 – Snowflake Hands-On Training Log
Platform, Ingestion & Native dbt Modeling

Account	`ua97434`
User	`SHASHI`
Date	2026-11-17
Edition	Enterprise trial (required for masking, row access policies, DMFs)
Repo	https://github.com/Shashisss/learn-snowflake
This log records every step performed, in order, including the errors we hit and how they were fixed.
Each command has a What it does explanation so it can be reused as training material.
---
Table of Contents
Step 1 – Trial Setup & Cost Governance
Step 2 – RBAC & PII Security
Step 3 – Git Integration & CREATE OR ALTER
Step 4a – Batch Ingestion: CSV with COPY INTO
Step 4b – Streaming Ingestion: JSON with Snowpipe + LATERAL FLATTEN
Concepts: Openflow & Fivetran
Errors & Lessons Learned (summary)
Object Inventory
What's Next
---
Step 1 – Trial Setup & Cost Governance
1.1 Register the trial (manual)
Sign up at signup.snowflake.com and choose Enterprise edition.
Why Enterprise: Dynamic Data Masking, Row Access Policies, tag-based policies and Data Metric Functions are Enterprise-only features.
1.2 Account-level resource monitor
```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR RM_ACCOUNT_TRIAL
  WITH CREDIT_QUOTA = 50
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75  PERCENT DO NOTIFY
    ON 90  PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER ACCOUNT SET RESOURCE_MONITOR = RM_ACCOUNT_TRIAL;
```
What it does
`USE ROLE ACCOUNTADMIN` – only ACCOUNTADMIN can create resource monitors.
`CREDIT_QUOTA = 50` – budget of 50 credits per period.
`FREQUENCY = MONTHLY` – the quota resets every month.
`START_TIMESTAMP = IMMEDIATELY` – start counting now.
Triggers:
`NOTIFY` – sends an email/alert only (enable in Profile → Notifications).
`SUSPEND` – suspends warehouses after running queries finish.
`SUSPEND_IMMEDIATE` – suspends and cancels running queries.
`ALTER ACCOUNT SET RESOURCE_MONITOR` – attaches the monitor to the whole account (only one account-level monitor allowed).
Result: ✅ Monitor created.
1.3 Warehouses (workload isolation)
```sql
CREATE WAREHOUSE IF NOT EXISTS WH_INGEST
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE WAREHOUSE IF NOT EXISTS WH_TRANSFORM
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE WAREHOUSE IF NOT EXISTS WH_ANALYTICS
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
```
What it does
One warehouse per workload, so costs can be tracked and capped separately.
`XSMALL` – smallest size, 1 credit/hour.
`AUTO_SUSPEND = 60` – shuts down after 60 seconds idle (billing is per second, 60-second minimum).
`AUTO_RESUME = TRUE` – starts automatically when a query arrives.
`INITIALLY_SUSPENDED = TRUE` – don't burn credits right after creation.
> In this session `WH_TRANSFORM` was actually created during Step 2 and used for all later work.
1.4 Per-warehouse monitor and guardrails (optional)
```sql
CREATE OR REPLACE RESOURCE MONITOR RM_TRANSFORM
  WITH CREDIT_QUOTA = 15 FREQUENCY = WEEKLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE WH_TRANSFORM SET RESOURCE_MONITOR = RM_TRANSFORM;

ALTER WAREHOUSE WH_TRANSFORM SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;
ALTER ACCOUNT SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
```
What it does
A tighter weekly cap for one warehouse (a warehouse can have only one monitor).
`STATEMENT_TIMEOUT_IN_SECONDS` – kills runaway queries after 30 min (warehouse) / 60 min (account).
1.5 Cost-attribution tags and spend query
```sql
CREATE DATABASE IF NOT EXISTS GOVERNANCE;
CREATE SCHEMA IF NOT EXISTS GOVERNANCE.TAGS;
CREATE TAG IF NOT EXISTS GOVERNANCE.TAGS.COST_CENTER ALLOWED_VALUES 'ingest','transform','analytics';
ALTER WAREHOUSE WH_TRANSFORM SET TAG GOVERNANCE.TAGS.COST_CENTER = 'transform';

SELECT warehouse_name, DATE_TRUNC('day', start_time) AS day, SUM(credits_used) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2 ORDER BY 2 DESC, 3 DESC;
```
What it does
Tags let you group spend by team or cost center (chargeback).
`WAREHOUSE_METERING_HISTORY` – credits used per warehouse per hour (up to ~3 h latency).
Key concept: resource monitors control warehouse compute only. Serverless features (Snowpipe, serverless tasks, Cortex) are not capped – use Budgets (Admin → Cost Management) for those.
---
Step 2 – RBAC & PII Security
Role design used
```
ACCOUNTADMIN
 └─ SYSADMIN
     ├─ DATA_ENGINEER ──► DB_RW (database role) ──► DB_RO
     │    └─ ANALYST ───► DB_RO
     ├─ CORTEX_USER ────► DB_RO + SNOWFLAKE.CORTEX_USER
     └─ HR_ADMIN ───────► DB_RO  (sees unmasked PII)
```
Functional roles (account level) = who you are (job function).
Access roles (database roles) = what you can do on objects.
Functional roles are granted access roles; users are granted functional roles.
2.1 Check the session context
```sql
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_USER();
```
What it does: confirms which role the session runs as before running privileged DDL.
Result: `ACCOUNTADMIN, COMPUTE_WH, SHASHI`
2.2 Create functional roles and hierarchy
```sql
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS DATA_ENGINEER COMMENT='Builds and owns pipelines';
CREATE ROLE IF NOT EXISTS CORTEX_USER   COMMENT='Uses Cortex AI functions';
CREATE ROLE IF NOT EXISTS ANALYST       COMMENT='Read-only consumer; sees masked PII';
CREATE ROLE IF NOT EXISTS HR_ADMIN      COMMENT='Sees unmasked PII';

GRANT ROLE ANALYST       TO ROLE DATA_ENGINEER;
GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE CORTEX_USER   TO ROLE SYSADMIN;
GRANT ROLE ANALYST       TO ROLE SYSADMIN;
GRANT ROLE HR_ADMIN      TO ROLE SYSADMIN;

GRANT ROLE DATA_ENGINEER TO USER SHASHI;
GRANT ROLE CORTEX_USER   TO USER SHASHI;
GRANT ROLE ANALYST       TO USER SHASHI;
GRANT ROLE HR_ADMIN      TO USER SHASHI;
```
What it does
`SECURITYADMIN` – the system role meant for managing roles and grants.
`GRANT ROLE ANALYST TO ROLE DATA_ENGINEER` – DATA_ENGINEER inherits everything ANALYST can do.
Granting every custom role to `SYSADMIN` is best practice: SYSADMIN can then manage all objects those roles create (no "orphaned" objects).
Granting to `USER SHASHI` lets the trainee switch into each role for testing.
Result: ✅ Success.
2.3 Create the database, schema and warehouse
```sql
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS STAFFING_DB;
CREATE SCHEMA IF NOT EXISTS STAFFING_DB.HR;
CREATE WAREHOUSE IF NOT EXISTS WH_TRANSFORM
  WAREHOUSE_SIZE=XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE;
```
What it does: SYSADMIN creates and therefore owns the database. Ownership matters in the next step.
2.4 ❌ First attempt at database roles – FAILED
```sql
USE ROLE SECURITYADMIN;
CREATE DATABASE ROLE IF NOT EXISTS STAFFING_DB.DB_RW;
```
Error
```
SQL access control error: Insufficient privileges to operate on database 'STAFFING_DB'.
Your primary role SECURITYADMIN must have CREATE DATABASE ROLE granted on DATABASE STAFFING_DB.
```
Why: database roles live inside a database. Only the database owner (SYSADMIN here), or a role granted `CREATE DATABASE ROLE` on that database, can create them. SECURITYADMIN manages account roles but does not own STAFFING_DB.
2.5 ✅ Database (access) roles – FIXED, run as the owner
```sql
USE ROLE SYSADMIN;
CREATE DATABASE ROLE IF NOT EXISTS STAFFING_DB.DB_RW;
CREATE DATABASE ROLE IF NOT EXISTS STAFFING_DB.DB_RO;

GRANT USAGE ON DATABASE STAFFING_DB              TO DATABASE ROLE STAFFING_DB.DB_RO;
GRANT USAGE ON ALL SCHEMAS IN DATABASE STAFFING_DB TO DATABASE ROLE STAFFING_DB.DB_RO;
GRANT DATABASE ROLE STAFFING_DB.DB_RO            TO DATABASE ROLE STAFFING_DB.DB_RW;
GRANT CREATE SCHEMA ON DATABASE STAFFING_DB      TO DATABASE ROLE STAFFING_DB.DB_RW;
GRANT ALL ON ALL SCHEMAS IN DATABASE STAFFING_DB TO DATABASE ROLE STAFFING_DB.DB_RW;

GRANT DATABASE ROLE STAFFING_DB.DB_RW TO ROLE DATA_ENGINEER;
GRANT DATABASE ROLE STAFFING_DB.DB_RO TO ROLE ANALYST;
GRANT DATABASE ROLE STAFFING_DB.DB_RO TO ROLE HR_ADMIN;
GRANT DATABASE ROLE STAFFING_DB.DB_RO TO ROLE CORTEX_USER;

GRANT USAGE, OPERATE ON WAREHOUSE WH_TRANSFORM TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE WH_TRANSFORM TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE WH_TRANSFORM TO ROLE HR_ADMIN;
GRANT USAGE ON WAREHOUSE WH_TRANSFORM TO ROLE CORTEX_USER;

USE ROLE ACCOUNTADMIN;
GRANT USAGE  ON FUTURE SCHEMAS IN DATABASE STAFFING_DB TO DATABASE ROLE STAFFING_DB.DB_RO;
GRANT SELECT ON FUTURE TABLES  IN DATABASE STAFFING_DB TO DATABASE ROLE STAFFING_DB.DB_RO;
GRANT SELECT ON FUTURE VIEWS   IN DATABASE STAFFING_DB TO DATABASE ROLE STAFFING_DB.DB_RO;
GRANT ALL    ON FUTURE SCHEMAS IN DATABASE STAFFING_DB TO DATABASE ROLE STAFFING_DB.DB_RW;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CORTEX_USER;
```
What it does
`DB_RO` – read-only access role; `DB_RW` inherits DB_RO and adds create/modify rights.
`USAGE` on database/schema – required just to "see into" the container.
`ON ALL ...` – applies to objects that exist now.
`ON FUTURE ...` – applies automatically to objects created later. Database-level future grants need `MANAGE GRANTS`, so they were run as ACCOUNTADMIN.
`USAGE` on a warehouse = can run queries; `OPERATE` = can also start/stop it.
`SNOWFLAKE.CORTEX_USER` – built-in database role that allows calling Cortex AI functions.
Result: ✅ Success.
2.6 Verify the database roles
```sql
SHOW DATABASE ROLES IN DATABASE STAFFING_DB;
```
Result
name	granted_to_roles	granted_to_database_roles	granted_database_roles	owner
DB_RO	3	1	0	SYSADMIN
DB_RW	1	0	1	SYSADMIN
DB_RO → ANALYST, HR_ADMIN, CORTEX_USER and DB_RW. DB_RW → DATA_ENGINEER and contains DB_RO.
2.7 Mock PII table
```sql
USE ROLE SYSADMIN;
CREATE OR REPLACE TABLE STAFFING_DB.HR.EMPLOYEES (
  emp_id INT, full_name STRING, ssn STRING, salary NUMBER(12,2), region STRING);

INSERT INTO STAFFING_DB.HR.EMPLOYEES VALUES
 (1,'Asha Rao','123-45-6789',95000,'WEST'),
 (2,'John Lee','987-65-4321',120000,'EAST'),
 (3,'Maria Diaz','555-12-3456',88000,'WEST'),
 (4,'Ken Ito','222-33-4444',135000,'EAST');

GRANT SELECT ON TABLE STAFFING_DB.HR.EMPLOYEES TO DATABASE ROLE STAFFING_DB.DB_RO;
```
What it does: creates fake employee data with two sensitive columns (SSN, salary) and a `region` column used for row-level security.
2.8 Object tag – classify sensitive columns
```sql
CREATE TAG IF NOT EXISTS STAFFING_DB.HR.PII_TYPE ALLOWED_VALUES 'SSN','SALARY';

ALTER TABLE STAFFING_DB.HR.EMPLOYEES MODIFY COLUMN ssn    SET TAG STAFFING_DB.HR.PII_TYPE = 'SSN';
ALTER TABLE STAFFING_DB.HR.EMPLOYEES MODIFY COLUMN salary SET TAG STAFFING_DB.HR.PII_TYPE = 'SALARY';
```
What it does
A tag is a key/value label on an object or column.
`ALLOWED_VALUES` restricts which values can be used (data hygiene).
A tag by itself does not protect anything – it becomes protection once a policy is attached to it.
2.9 Dynamic Data Masking (tag-based)
```sql
CREATE OR REPLACE MASKING POLICY STAFFING_DB.HR.MASK_SSN AS (v STRING) RETURNS STRING ->
  CASE WHEN IS_ROLE_IN_SESSION('HR_ADMIN') THEN v
       ELSE 'XXX-XX-' || RIGHT(v, 4) END;

CREATE OR REPLACE MASKING POLICY STAFFING_DB.HR.MASK_SALARY AS (v NUMBER(12,2)) RETURNS NUMBER(12,2) ->
  CASE WHEN IS_ROLE_IN_SESSION('HR_ADMIN') THEN v ELSE NULL END;

ALTER TAG STAFFING_DB.HR.PII_TYPE SET
  MASKING POLICY STAFFING_DB.HR.MASK_SSN,
  MASKING POLICY STAFFING_DB.HR.MASK_SALARY;
```
What it does
A masking policy is a function evaluated at query time; stored data is never changed.
`IS_ROLE_IN_SESSION('HR_ADMIN')` – true if HR_ADMIN is the current role or inherited. Safer than `CURRENT_ROLE()`.
`ALTER TAG ... SET MASKING POLICY` – tag-based masking: every column carrying the tag is masked automatically, by matching data type (STRING → MASK_SSN, NUMBER → MASK_SALARY).
Rule: one masking policy per data type per tag.
> To re-run with `CREATE OR REPLACE`, first detach:
> `ALTER TAG STAFFING_DB.HR.PII_TYPE UNSET MASKING POLICY STAFFING_DB.HR.MASK_SSN;`
2.10 Row Access Policy
```sql
CREATE OR REPLACE TABLE STAFFING_DB.HR.REGION_ACCESS (role_name STRING, region STRING);
INSERT INTO STAFFING_DB.HR.REGION_ACCESS VALUES ('ANALYST','WEST');

CREATE OR REPLACE ROW ACCESS POLICY STAFFING_DB.HR.RAP_REGION AS (r STRING) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('HR_ADMIN')
  OR IS_ROLE_IN_SESSION('DATA_ENGINEER')
  OR EXISTS (SELECT 1 FROM STAFFING_DB.HR.REGION_ACCESS a
             WHERE a.role_name = CURRENT_ROLE() AND a.region = r);

ALTER TABLE STAFFING_DB.HR.EMPLOYEES ADD ROW ACCESS POLICY STAFFING_DB.HR.RAP_REGION ON (region);
```
What it does
`REGION_ACCESS` – a mapping table: which role may see which region. Access changes are data changes, not policy changes.
The policy returns TRUE/FALSE per row; rows returning FALSE are invisible.
`ON (region)` – passes each row's `region` value into the policy as `r`.
When re-running: `ALTER TABLE ... DROP ALL ROW ACCESS POLICIES;` first.
2.11 Test as different roles
```sql
USE SECONDARY ROLES NONE;
USE ROLE HR_ADMIN; USE WAREHOUSE WH_TRANSFORM;
SELECT CURRENT_ROLE() r, * FROM STAFFING_DB.HR.EMPLOYEES;

USE SECONDARY ROLES NONE;
USE ROLE ANALYST; USE WAREHOUSE WH_TRANSFORM;
SELECT CURRENT_ROLE() r, * FROM STAFFING_DB.HR.EMPLOYEES;
```
What it does
`USE SECONDARY ROLES NONE` – important: the account defaults to secondary roles = ALL, meaning every role granted to the user is active. Since SHASHI also has HR_ADMIN, ANALYST would otherwise see unmasked data.
Result – HR_ADMIN (4 rows, unmasked)
EMP_ID	FULL_NAME	SSN	SALARY	REGION
1	Asha Rao	123-45-6789	95000.00	WEST
2	John Lee	987-65-4321	120000.00	EAST
3	Maria Diaz	555-12-3456	88000.00	WEST
4	Ken Ito	222-33-4444	135000.00	EAST
Result – ANALYST (2 WEST rows, masked)
EMP_ID	FULL_NAME	SSN	SALARY	REGION
1	Asha Rao	XXX-XX-6789	NULL	WEST
3	Maria Diaz	XXX-XX-3456	NULL	WEST
2.12 Inspect policy attachments
```sql
USE ROLE ACCOUNTADMIN;
SELECT * FROM TABLE(STAFFING_DB.INFORMATION_SCHEMA.POLICY_REFERENCES(
  REF_ENTITY_NAME => 'STAFFING_DB.HR.EMPLOYEES', REF_ENTITY_DOMAIN => 'TABLE'));
USE SECONDARY ROLES ALL;
```
What it does: lists every masking and row access policy attached to the table, and whether it came from a tag. `USE SECONDARY ROLES ALL` restores the default.
2.13 Exercise – extend access and add an EMAIL column
```sql
USE ROLE SYSADMIN;

-- (a) give ANALYST the EAST region as well
INSERT INTO STAFFING_DB.HR.REGION_ACCESS VALUES ('ANALYST','EAST');

-- (b) allow a new tag value
ALTER TAG STAFFING_DB.HR.PII_TYPE ADD ALLOWED_VALUES 'EMAIL';

-- (c) new PII column with data
ALTER TABLE STAFFING_DB.HR.EMPLOYEES ADD COLUMN email STRING;
UPDATE STAFFING_DB.HR.EMPLOYEES SET email = LOWER(REPLACE(full_name,' ','.')) || '@acme.com';

-- (d) third masking policy
CREATE OR REPLACE MASKING POLICY STAFFING_DB.HR.MASK_EMAIL AS (v STRING) RETURNS STRING ->
  CASE WHEN IS_ROLE_IN_SESSION('HR_ADMIN') THEN v
       ELSE '*****@' || SPLIT_PART(v,'@',2) END;

-- (e) tag + direct policy
ALTER TABLE STAFFING_DB.HR.EMPLOYEES MODIFY COLUMN email SET TAG STAFFING_DB.HR.PII_TYPE = 'EMAIL';
ALTER TABLE STAFFING_DB.HR.EMPLOYEES MODIFY COLUMN email SET MASKING POLICY STAFFING_DB.HR.MASK_EMAIL;
```
What it does / why
(a) No policy change needed – only the mapping row. ANALYST now sees all 4 rows.
(b) Tags with `ALLOWED_VALUES` reject values not in the list.
(c) `REPLACE` + `LOWER` builds `asha.rao@acme.com`, etc.
(d) Keeps the domain, hides the local part: `*****@acme.com`.
(e) Gotcha: the tag already maps STRING → `MASK_SSN`. Without a direct policy, email would be masked as `XXX-XX-.com`. A policy set directly on the column overrides the tag-based one.
Fully tag-driven alternative: one STRING policy that branches on `SYSTEM$GET_TAG_ON_CURRENT_COLUMN('STAFFING_DB.HR.PII_TYPE')`.
Expected result: ANALYST → 4 rows, SSN `XXX-XX-####`, salary NULL, email `*****@acme.com`. HR_ADMIN → everything clear.
(The assistant's attempt to run this was declined; the trainee ran it in the worksheet.)
PII in brief
Tag = label. Masking policy = query-time function returning the real or masked value. Tag-based masking = attach the policy to the tag → auto-applied. Row access policy = TRUE/FALSE per row.
---
Step 3 – Git Integration & CREATE OR ALTER
3.1 Credentials question: "I sign in to GitHub with Gmail – what password?"
Snowflake never uses your GitHub login password. It needs a Personal Access Token (PAT).
Create one: GitHub → Settings → Developer settings → Personal access tokens → Fine-grained → select repo → Contents: Read-only → Generate.
`USERNAME` in the secret is your GitHub username, not the Gmail address.
Never paste a token into chat; type it directly into the worksheet.
Public repos need no credentials – this repo is public, so no secret was created.
Private-repo version (not used):
```sql
CREATE OR REPLACE SECRET STAFFING_DB.OPS.GH_SECRET
  TYPE = PASSWORD USERNAME = '<github_user>' PASSWORD = '<github_pat>';
```
3.2 API integration + Git repository object
```sql
USE ROLE ACCOUNTADMIN;
CREATE SCHEMA IF NOT EXISTS STAFFING_DB.OPS;

CREATE OR REPLACE API INTEGRATION GIT_API_INT
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ('https://github.com/Shashisss')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY STAFFING_DB.OPS.STAFFING_REPO
  API_INTEGRATION = GIT_API_INT
  ORIGIN = 'https://github.com/Shashisss/learn-snowflake.git';

ALTER GIT REPOSITORY STAFFING_DB.OPS.STAFFING_REPO FETCH;
SHOW GIT BRANCHES IN STAFFING_DB.OPS.STAFFING_REPO;
```
What it does
`API INTEGRATION` (account-level, ACCOUNTADMIN) – allows Snowflake to make outbound HTTPS calls to GitHub, restricted to URLs starting with the allowed prefix.
`GIT REPOSITORY` – a read-only clone of the repo inside Snowflake, browsable like a stage.
`FETCH` – pulls the latest commits from GitHub.
`SHOW GIT BRANCHES` – lists branches and their latest commit hash.
Result: objects created, but 0 branches.
3.3 ❌ Browsing an empty repo – FAILED
```sql
LS @STAFFING_DB.OPS.STAFFING_REPO/branches/;
```
Error: `Files paths in git repositories must specify a scope. For example, a branch name, a tag name, or a valid commit hash.`
Why: you must list a specific branch/tag/commit (`/branches/main/`), and here the repo was empty – nothing had been committed yet, so no `main` branch existed.
Fix: on GitHub, create `sql/bronze_tables.sql` and commit to `main`:
```sql
CREATE SCHEMA IF NOT EXISTS STAFFING_DB.BRONZE;

CREATE OR ALTER TABLE STAFFING_DB.BRONZE.CANDIDATES (
  candidate_id INT,
  full_name    STRING,
  email        STRING
);
```
3.4 Deploy from Git
```sql
ALTER GIT REPOSITORY STAFFING_DB.OPS.STAFFING_REPO FETCH;
SHOW GIT BRANCHES IN STAFFING_DB.OPS.STAFFING_REPO;
LS @STAFFING_DB.OPS.STAFFING_REPO/branches/main/;
EXECUTE IMMEDIATE FROM @STAFFING_DB.OPS.STAFFING_REPO/branches/main/sql/bronze_tables.sql;
DESC TABLE STAFFING_DB.BRONZE.CANDIDATES;
```
What it does
`LS` – lists files on the `main` branch.
`EXECUTE IMMEDIATE FROM` – runs every SQL statement in the file straight from the repo.
`DESC TABLE` – shows the resulting columns.
Result: table created with 3 columns.
3.5 Declarative change with CREATE OR ALTER
Edited the file on GitHub to add `created_at TIMESTAMP_NTZ`, committed, then checked:
```sql
DESC TABLE STAFFING_DB.BRONZE.CANDIDATES;
```
Observation: still 3 columns – the new commit had not been fetched and executed yet.
Read the file straight from the repo to confirm the commit had arrived:
```sql
ALTER GIT REPOSITORY STAFFING_DB.OPS.STAFFING_REPO FETCH;
SELECT $1 FROM @STAFFING_DB.OPS.STAFFING_REPO/branches/main/sql/bronze_tables.sql
  (FILE_FORMAT => (TYPE=CSV FIELD_DELIMITER=NONE));
```
❌ Error: `syntax error ... unexpected 'FIELD_DELIMITER'` – inline format options are not allowed inside the `FILE_FORMAT =>` table argument; it only accepts the name of a file format.
✅ Fix:
```sql
CREATE OR REPLACE FILE FORMAT STAFFING_DB.OPS.FF_RAW TYPE=CSV FIELD_DELIMITER=NONE;
SELECT $1 FROM @STAFFING_DB.OPS.STAFFING_REPO/branches/main/sql/bronze_tables.sql
  (FILE_FORMAT => 'STAFFING_DB.OPS.FF_RAW');
```
`FIELD_DELIMITER=NONE` returns each line as one column – a handy way to "cat" a file in a stage. Confirmed the `created_at` line was present.
Then applied it:
```sql
EXECUTE IMMEDIATE FROM @STAFFING_DB.OPS.STAFFING_REPO/branches/main/sql/bronze_tables.sql;
DESC TABLE STAFFING_DB.BRONZE.CANDIDATES;
```
Result
Column	Type
CANDIDATE_ID	NUMBER(38,0)
FULL_NAME	VARCHAR
EMAIL	VARCHAR
CREATED_AT	TIMESTAMP_NTZ(9) ← added in place
Key concept
`CREATE OR REPLACE` drops and recreates → data lost.
`CREATE OR ALTER` compares the definition to the live table and applies only the difference (adds/drops columns) → data kept. The file is the desired state (declarative, like Terraform).
Deploy loop: commit to GitHub → FETCH → EXECUTE IMMEDIATE FROM.
3.6 Commit history
```sql
ALTER GIT REPOSITORY STAFFING_DB.OPS.STAFFING_REPO FETCH;
SHOW GIT BRANCHES IN STAFFING_DB.OPS.STAFFING_REPO;
SHOW GIT TAGS IN STAFFING_DB.OPS.STAFFING_REPO;
LS @STAFFING_DB.OPS.STAFFING_REPO/commits/926a25b/;
```
Result: `main` → commit `926a25b62b9dc69376ea1f27f8101f06a310ac19`.
Note: Snowflake exposes only the latest commit per branch/tag, not a full `git log`. Full history is on GitHub; older versions can be read via `/commits/<hash>/`.
3.7 Data check after deploy
```sql
SELECT COUNT(*) AS total_rows, COUNT_IF(email IS NULL) AS null_emails
FROM STAFFING_DB.BRONZE.CANDIDATES;
```
Result: `0, NULL` – Git deploys create structure only; data comes from ingestion (Step 4).
`COUNT_IF` returns NULL on an empty table here; use `COALESCE(COUNT_IF(...),0)` if you need 0.
---
Step 4a – Batch Ingestion: CSV with COPY INTO
4a.1 File format, stage, sample CSV and load
```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE FILE FORMAT STAFFING_DB.BRONZE.FF_CSV
  TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF=('','NULL');

CREATE STAGE IF NOT EXISTS STAFFING_DB.BRONZE.RAW_STAGE FILE_FORMAT = STAFFING_DB.BRONZE.FF_CSV;

-- Generate a CSV file on the stage (no local file upload needed)
COPY INTO @STAFFING_DB.BRONZE.RAW_STAGE/candidates/candidates_001.csv
FROM (SELECT * FROM VALUES
  (1,'Asha Rao','asha.rao@mail.com','2026-11-01 09:00:00'),
  (2,'John Lee','john.lee@mail.com','2026-11-02 10:30:00'),
  (3,'Maria Diaz',NULL,'2026-11-03 14:15:00'),
  (4,'Ken Ito','ken.ito@mail.com','2026-11-04 08:45:00'),
  (5,'Priya Nair','priya.nair@mail.com','2026-11-05 16:20:00')
  AS t(candidate_id,full_name,email,created_at))
FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE)
HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE;

-- Load it into the Bronze table
COPY INTO STAFFING_DB.BRONZE.CANDIDATES (candidate_id, full_name, email, created_at)
FROM @STAFFING_DB.BRONZE.RAW_STAGE/candidates/
FILE_FORMAT = (FORMAT_NAME = 'STAFFING_DB.BRONZE.FF_CSV')
ON_ERROR = 'ABORT_STATEMENT';
```
What it does
File format – reusable parsing rules: skip the header row, optional double quotes, treat `''`/`'NULL'` as NULL.
Internal stage – Snowflake-managed file storage (like an S3 folder).
`COPY INTO @stage FROM (SELECT ...)` – unload: writes query results to a file. Used here to create test data without uploading anything.
`HEADER = TRUE` writes column names; `SINGLE = TRUE` writes one file with the exact name; `OVERWRITE = TRUE` replaces it if present.
`COPY INTO table FROM @stage` – load: reads every file under the path.
`ON_ERROR = 'ABORT_STATEMENT'` – fail the whole load on any bad row (alternatives: `CONTINUE`, `SKIP_FILE`).
Result: `candidates_001.csv | LOADED | rows_parsed 5 | rows_loaded 5 | errors 0`
4a.2 ❌ Duplicates found
```sql
SELECT COUNT(*) AS total_rows, COUNT_IF(email IS NULL) AS null_emails FROM STAFFING_DB.BRONZE.CANDIDATES;
-- 10, 2

SELECT candidate_id, full_name, email, COUNT(*) AS copies
FROM STAFFING_DB.BRONZE.CANDIDATES GROUP BY ALL ORDER BY 1;
-- every candidate appeared 2×
```
Why: the file was loaded twice (the same script was also run from the worksheet). COPY keeps load metadata (64 days) and skips files it has already loaded – but it identifies files by name and checksum/ETag. Re-creating the file with `OVERWRITE = TRUE` produced a "new" file, so it was loaded again.
Lesson: Bronze is append-only and will contain duplicates in real life – deduplication belongs in Silver.
4a.3 ✅ Reset to one clean copy
```sql
TRUNCATE TABLE STAFFING_DB.BRONZE.CANDIDATES;

COPY INTO STAFFING_DB.BRONZE.CANDIDATES (candidate_id, full_name, email, created_at)
FROM @STAFFING_DB.BRONZE.RAW_STAGE/candidates/
FILE_FORMAT = (FORMAT_NAME = 'STAFFING_DB.BRONZE.FF_CSV')
FORCE = TRUE;

SELECT COUNT(*) AS total_rows, COUNT(DISTINCT candidate_id) AS distinct_ids,
       COUNT_IF(email IS NULL) AS null_emails
FROM STAFFING_DB.BRONZE.CANDIDATES;
```
What it does
`TRUNCATE` – deletes all rows (keeps the table and grants).
`FORCE = TRUE` – load the file even though load metadata says it's already loaded. Use carefully – it is the easiest way to create duplicates.
Result: `5 rows | 5 distinct IDs | 1 null email (Maria Diaz)` ✅
4a.4 Inspect the stage
```sql
LIST @STAFFING_DB.BRONZE.RAW_STAGE;

SELECT METADATA$FILENAME AS file, COUNT(*) AS data_rows
FROM @STAFFING_DB.BRONZE.RAW_STAGE (FILE_FORMAT => 'STAFFING_DB.BRONZE.FF_CSV')
GROUP BY 1;
```
What it does
`LIST` – file name, size, MD5, last-modified.
Querying a stage directly reads the files without loading them. `METADATA$FILENAME` is a pseudo-column with the source file name.
Result: `candidates/candidates_001.csv | 224 bytes | 5 data rows`
Note: a stage holds files, not rows; rows are counted by reading the file.
---
Step 4b – Streaming Ingestion: JSON with Snowpipe + LATERAL FLATTEN
4b.1 JSON format and raw landing table
```sql
CREATE OR REPLACE FILE FORMAT STAFFING_DB.BRONZE.FF_JSON TYPE = JSON STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE TABLE STAFFING_DB.BRONZE.REQUISITIONS_RAW (
  raw        VARIANT,
  file_name  STRING,
  loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```
What it does
`STRIP_OUTER_ARRAY = TRUE` – if a file is one big `[ {...}, {...} ]`, each element becomes its own row.
`VARIANT` – semi-structured column that stores any JSON as-is (schema-on-read).
`file_name` and `loaded_at` – lineage/audit columns, standard in Bronze.
4b.2 ❌ Generating the JSON file – FAILED
Run in the worksheet:
```sql
USE ROLE ACCOUNTADMIN;      -- ❌ Command not supported by SQL API: USE
USE WAREHOUSE WH_TRANSFORM; -- ❌ Command not supported by SQL API: USE
...
COPY INTO @STAFFING_DB.BRONZE.RAW_STAGE/requisitions/req_001.json
FROM (SELECT OBJECT_CONSTRUCT(..., 'skills', skills) FROM VALUES
  (101,'Data Engineer','Acme','Austin', ARRAY_CONSTRUCT('SQL','Python','Snowflake')), ...)
-- ❌ SQL compilation error: Invalid expression [ARRAY_CONSTRUCT(...)] in VALUES clause
```
Why
`USE` errors – that execution path (SQL API) doesn't accept `USE`. Harmless: the worksheet context already set the role and warehouse. The file format and table (4b.1) still succeeded.
`VALUES` accepts constants only – function calls such as `ARRAY_CONSTRUCT` are not allowed there.
4b.3 ✅ Fixed – pass a string, convert with SPLIT
```sql
COPY INTO @STAFFING_DB.BRONZE.RAW_STAGE/requisitions/req_001.json
FROM (SELECT OBJECT_CONSTRUCT(
         'req_id', id,
         'title',  title,
         'client', OBJECT_CONSTRUCT('name', client, 'city', city),
         'skills', SPLIT(skills, ',')) FROM VALUES
  (101,'Data Engineer','Acme','Austin',  'SQL,Python,Snowflake'),
  (102,'BI Analyst',   'Globex','Denver','SQL,Tableau'),
  (103,'ML Engineer',  'Initech','NYC',  'Python,Spark,MLOps')
  AS t(id,title,client,city,skills))
FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE) SINGLE = TRUE OVERWRITE = TRUE;
```
What it does
`OBJECT_CONSTRUCT` – builds a JSON object from key/value pairs; nesting it creates the `client` sub-object.
`SPLIT(str, ',')` – turns `'SQL,Python'` into the array `["SQL","Python"]`. Functions are fine in the SELECT, just not in VALUES.
`COMPRESSION = NONE` – second gotcha: the first successful run gzipped the output (338 → 197 bytes) while keeping the `.json` name. Writing it uncompressed avoids confusing loaders. Re-run result: 338 → 338 bytes.
Example record produced:
```json
{"req_id":101,"title":"Data Engineer","client":{"name":"Acme","city":"Austin"},"skills":["SQL","Python","Snowflake"]}
```
4b.4 Snowpipe
```sql
CREATE OR REPLACE PIPE STAFFING_DB.BRONZE.PIPE_REQUISITIONS AS
COPY INTO STAFFING_DB.BRONZE.REQUISITIONS_RAW (raw, file_name)
FROM (SELECT $1, METADATA$FILENAME FROM @STAFFING_DB.BRONZE.RAW_STAGE/requisitions/)
FILE_FORMAT = (FORMAT_NAME = 'STAFFING_DB.BRONZE.FF_JSON');

ALTER PIPE STAFFING_DB.BRONZE.PIPE_REQUISITIONS REFRESH;
SELECT SYSTEM$PIPE_STATUS('STAFFING_DB.BRONZE.PIPE_REQUISITIONS');
```
What it does
A pipe wraps a COPY statement and runs it on serverless compute whenever new files arrive – no warehouse needed.
`$1` – the whole JSON document; `METADATA$FILENAME` – source file name.
`REFRESH` – queues files already on the stage (last 7 days). Needed for internal stages, which can't send event notifications.
On S3/Azure/GCS, add `AUTO_INGEST = TRUE` and cloud event notifications trigger the pipe automatically.
`SYSTEM$PIPE_STATUS` – JSON with state, pending file count and last ingested file.
What happened
First check: `REQUISITIONS_RAW` had 0 rows, pipe `RUNNING`, `pendingFileCount 0`. The earlier REFRESH had run before the JSON file was rewritten.
Ran `REFRESH` again → `req_001.json | SENT`.
`COPY_HISTORY` was empty right away – Snowpipe is asynchronous (seconds to a minute).
Waited ~45 seconds → data loaded.
```sql
SELECT file_name, status, row_count, first_error_message, last_load_time
FROM TABLE(STAFFING_DB.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'STAFFING_DB.BRONZE.REQUISITIONS_RAW',
  START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP())));
```
`COPY_HISTORY` shows per-file load results for both COPY and Snowpipe – the first place to look when a load "didn't happen".
4b.5 Parse VARIANT – dot notation and LATERAL FLATTEN
```sql
-- Nested object fields
SELECT raw:req_id::INT          AS req_id,
       raw:title::STRING        AS title,
       raw:client.name::STRING  AS client_name,
       raw:client.city::STRING  AS client_city
FROM STAFFING_DB.BRONZE.REQUISITIONS_RAW;

-- Explode the array: one row per skill
SELECT r.raw:req_id::INT   AS req_id,
       r.raw:title::STRING AS title,
       r.raw:client.name::STRING AS client,
       s.index             AS skill_pos,
       s.value::STRING     AS skill
FROM STAFFING_DB.BRONZE.REQUISITIONS_RAW r,
     LATERAL FLATTEN(INPUT => r.raw:skills) s
ORDER BY req_id, skill_pos;
```
What it does
`raw:field` – reads a top-level key; `raw:client.name` – reads a nested key.
`::INT`, `::STRING` – casts the VARIANT value to a SQL type (otherwise strings come back quoted).
`LATERAL FLATTEN(INPUT => array)` – a table function producing one row per array element; `LATERAL` lets it reference the current row `r`.
Output columns: `index` (position), `value` (element), plus `key`, `path`, `seq`, `this`.
Result – 8 rows ✅
REQ_ID	TITLE	CLIENT	SKILL_POS	SKILL
101	Data Engineer	Acme	0	SQL
101	Data Engineer	Acme	1	Python
101	Data Engineer	Acme	2	Snowflake
102	BI Analyst	Globex	0	SQL
102	BI Analyst	Globex	1	Tableau
103	ML Engineer	Initech	0	Python
103	ML Engineer	Initech	1	Spark
103	ML Engineer	Initech	2	MLOps
4b.6 Pipe definition and status
```sql
SHOW PIPES LIKE 'PIPE_REQUISITIONS' IN SCHEMA STAFFING_DB.BRONZE;
SELECT SYSTEM$PIPE_STATUS('STAFFING_DB.BRONZE.PIPE_REQUISITIONS');
```
Result
Property	Value
Owner	ACCOUNTADMIN
Created	2026-11-17 09:31:36 -0800
Notification channel	none (internal stage → manual REFRESH)
executionState	RUNNING
pendingFileCount	0
lastIngestedFilePath	req_001.json
lastIngestedTimestamp	2026-11-17 17:39:21 UTC
Pause / resume:
```sql
ALTER PIPE STAFFING_DB.BRONZE.PIPE_REQUISITIONS SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE STAFFING_DB.BRONZE.PIPE_REQUISITIONS SET PIPE_EXECUTION_PAUSED = FALSE;
```
4b.7 Load a second file
```sql
ALTER PIPE STAFFING_DB.BRONZE.PIPE_REQUISITIONS SET PIPE_EXECUTION_PAUSED = FALSE;

COPY INTO @STAFFING_DB.BRONZE.RAW_STAGE/requisitions/req_002.json
FROM (SELECT OBJECT_CONSTRUCT('req_id', id, 'title', title,
         'client', OBJECT_CONSTRUCT('name', client, 'city', city),
         'skills', SPLIT(skills, ',')) FROM VALUES
  (104,'Cloud Architect','Umbrella','Seattle','AWS,Terraform,Snowflake'),
  (105,'Data Scientist','Hooli','San Jose','Python,SQL,Statistics,ML')
  AS t(id,title,client,city,skills))
FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE) SINGLE = TRUE OVERWRITE = TRUE;

ALTER PIPE STAFFING_DB.BRONZE.PIPE_REQUISITIONS REFRESH;
-- req_001.json SENT, req_002.json SENT
```
After ~45 seconds:
```sql
SELECT file_name, COUNT(*) AS reqs,
       ARRAY_AGG(raw:req_id::INT) WITHIN GROUP (ORDER BY raw:req_id::INT) AS req_ids
FROM STAFFING_DB.BRONZE.REQUISITIONS_RAW
GROUP BY 1 ORDER BY 1;
```
Result ✅
FILE_NAME	REQS	REQ_IDS
requisitions/req_001.json	3	[101,102,103]
requisitions/req_002.json	2	[104,105]
Lesson: REFRESH sent both files, but Snowpipe's load history skipped req_001.json (already loaded) – no duplicates. Contrast with 4a.2, where an overwritten file looked new.
---
Concepts: Openflow & Fivetran
(Review only – nothing run.)
Tool	What it is	Key ideas
Snowflake Openflow	Managed Apache NiFi inside Snowflake	Visual flows; connectors for database CDC (Postgres, MySQL, SQL Server), SaaS apps, Kafka; runs in Snowflake or your cloud (BYOC)
Fivetran	Managed ELT SaaS	Billed on MAR (Monthly Active Rows – each distinct row inserted/updated in a month counts once); handles schema drift by auto-adding new source columns; soft deletes flagged with `_fivetran_deleted`; `_fivetran_synced` timestamp
Choosing an ingestion method
Need	Use
Scheduled bulk files	`COPY INTO` (warehouse)
Files arriving continuously	Snowpipe (serverless, auto-ingest)
Row-level low-latency streaming	Snowpipe Streaming / Kafka connector
Database CDC / SaaS sources	Openflow or Fivetran
---
Errors & Lessons Learned (summary)
#	Where	Error / Symptom	Cause	Fix
1	2.4	`SECURITYADMIN must have CREATE DATABASE ROLE`	Database roles can only be created by the database owner	Run as SYSADMIN (owner)
2	2.11	(risk) ANALYST could see unmasked data	Secondary roles = ALL activates HR_ADMIN too	`USE SECONDARY ROLES NONE` when testing
3	2.13	(risk) email masked as `XXX-XX-.com`	One masking policy per data type per tag	Set a direct policy on the column (overrides tag)
4	3.3	`File paths in git repositories must specify a scope`	Must list a branch path; repo was empty	Commit a file; use `/branches/main/`
5	3.5	DESC still showed 3 columns	New commit not fetched/executed	`FETCH` + `EXECUTE IMMEDIATE FROM`
6	3.5	`unexpected 'FIELD_DELIMITER'`	`FILE_FORMAT =>` accepts a named format only	Create a named file format
7	4a.2	10 rows instead of 5	Overwritten file has a new checksum → loaded again	`TRUNCATE` + `FORCE=TRUE`; dedup in Silver
8	4b.2	`Command not supported by SQL API: USE`	Execution path rejects `USE`	Rely on worksheet context; skip `USE`
9	4b.2	`Invalid expression [ARRAY_CONSTRUCT] in VALUES`	VALUES accepts constants only	Pass a string, `SPLIT()` in SELECT
10	4b.3	Output smaller than input (gzipped .json)	JSON unload compresses by default	`COMPRESSION = NONE`
11	4b.4	Pipe RUNNING but table empty	REFRESH ran before the file existed; Snowpipe is async	REFRESH again, wait, check `COPY_HISTORY`
---
Object Inventory
Object	Type	Purpose
`RM_ACCOUNT_TRIAL`	Resource monitor	50 credits/month account cap
`WH_TRANSFORM`	Warehouse (XS)	All workloads in this session
`DATA_ENGINEER`, `CORTEX_USER`, `ANALYST`, `HR_ADMIN`	Account roles	Functional roles
`STAFFING_DB.DB_RW`, `STAFFING_DB.DB_RO`	Database roles	Access roles
`STAFFING_DB.HR.EMPLOYEES`	Table	Mock PII data
`STAFFING_DB.HR.REGION_ACCESS`	Table	Role → region mapping
`STAFFING_DB.HR.PII_TYPE`	Tag	SSN / SALARY / EMAIL
`STAFFING_DB.HR.MASK_SSN`, `MASK_SALARY`, `MASK_EMAIL`	Masking policies	PII masking
`STAFFING_DB.HR.RAP_REGION`	Row access policy	Region filtering
`GIT_API_INT`	API integration	GitHub access
`STAFFING_DB.OPS.STAFFING_REPO`	Git repository	Clone of learn-snowflake
`STAFFING_DB.OPS.FF_RAW`	File format	Read files line by line
`STAFFING_DB.BRONZE.CANDIDATES`	Table	5 candidates (CSV)
`STAFFING_DB.BRONZE.FF_CSV`, `FF_JSON`	File formats	CSV / JSON parsing
`STAFFING_DB.BRONZE.RAW_STAGE`	Internal stage	`candidates/`, `requisitions/`
`STAFFING_DB.BRONZE.REQUISITIONS_RAW`	Table (VARIANT)	5 requisitions (JSON)
`STAFFING_DB.BRONZE.PIPE_REQUISITIONS`	Pipe	Snowpipe for requisitions
---
What's Next
Incremental pipelines – Streams + Tasks + MERGE vs Dynamic Tables (`TARGET_LAG`); trade-offs against dbt.
Native dbt Project on Snowflake – staffing/ATS domain (Candidates, Requisitions, Placements, Timesheets) across Bronze → Silver (dedup, soft deletes) → Gold (star schema, KPIs).
SCD Type 2 & Data Quality – dbt Snapshots, incremental models, dbt tests and Data Metric Functions (DMFs).
Checking this log into the repo
The Snowflake Git repository object is read-only (FETCH only – it cannot push). To add this file:
GitHub → `learn-snowflake` → Add file → Upload files → drop `week1_session_log.md` (e.g. in `docs/`) → Commit changes.
In Snowflake:
```sql
ALTER GIT REPOSITORY STAFFING_DB.OPS.STAFFING_REPO FETCH;
LS @STAFFING_DB.OPS.STAFFING_REPO/branches/main/;
```