-- =====================================================================================
-- CORRECTED MERGE STATEMENT FOR DEPT_APPT TEMPORAL TABLE
-- =====================================================================================
-- This is how the tgtapptdeptinstera__ldapptdeptins.sql model should be updated
-- to properly handle temporal data with EFFT_D/EXPY_D patterns
-- =====================================================================================

{{
  config(
    post_hook=[
	'MERGE INTO '~cvar("mart_db")~'.'~cvar("gdw_acct_db")~'.'~cvar("tgt_table")~' AS target
USING {{ this }} AS source
ON target.appt_i = source.APPT_I AND target.dept_i = source.DEPT_I

-- 1. EXPIRE EXISTING ACTIVE ROWS THAT HAVE CHANGED
WHEN MATCHED 
    AND target.expy_d = TO_DATE(''9999-12-31'', ''YYYY-MM-DD'')  -- Only active rows
    AND (target.dept_role_c != source.DEPT_ROLE_C)  -- Only if data changed
THEN UPDATE SET
    target.expy_d = TO_DATE(''{{ cvar("etl_process_dt") }}'', ''YYYYMMDD'') - 1,  -- Expire yesterday
    target.pros_key_expy_i = {{ cvar("refr_pk") }}  -- Set expiry process key

-- 2. INSERT NEW ROWS (both completely new and updates to expired rows)
WHEN NOT MATCHED THEN
    INSERT (
        appt_i,
        dept_role_c,
        efft_d,
        dept_i,
        expy_d,
        pros_key_efft_i,
        pros_key_expy_i,
        eror_seqn_i
    )
    VALUES (
        source.APPT_I,
        source.DEPT_ROLE_C,
        source.EFFT_D,
        source.DEPT_I,
        source.EXPY_D,
        source.PROS_KEY_EFFT_I,
        source.PROS_KEY_EXPY_I,
        source.EROR_SEQN_I
    );'
	 ]
  )
}}

SELECT
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I

FROM {{ ref('tgtapptdeptinsertds__ldapptdeptins') }}

-- =====================================================================================
-- ALTERNATIVE APPROACH: TWO-STEP MERGE FOR COMPLEX TEMPORAL LOGIC
-- =====================================================================================
-- If the above single MERGE is too complex, consider this two-step approach:
--
-- STEP 1: Expire existing rows (separate UPDATE statement in pre_hook)
-- 'UPDATE '~cvar("mart_db")~'.'~cvar("gdw_acct_db")~'.'~cvar("tgt_table")~' 
--  SET expy_d = TO_DATE(''{{ cvar("etl_process_dt") }}'', ''YYYYMMDD'') - 1,
--      pros_key_expy_i = {{ cvar("refr_pk") }}
--  WHERE (appt_i, dept_i) IN (
--      SELECT APPT_I, DEPT_I FROM {{ this }}
--  )
--  AND expy_d = TO_DATE(''9999-12-31'', ''YYYY-MM-DD'');'
--
-- STEP 2: Insert all new rows (in post_hook)
-- 'INSERT INTO '~cvar("mart_db")~'.'~cvar("gdw_acct_db")~'.'~cvar("tgt_table")~'
--  SELECT * FROM {{ this }};'
--
-- =====================================================================================

-- =====================================================================================
-- EXPLANATION OF THE TEMPORAL PATTERN:
-- =====================================================================================
-- 1. EFFT_D (Effective Date): When the row becomes active
-- 2. EXPY_D (Expiry Date): When the row expires (9999-12-31 = currently active)
-- 3. PROS_KEY_EFFT_I: Process key when row was created
-- 4. PROS_KEY_EXPY_I: Process key when row was expired (NULL for active rows)
--
-- WORKFLOW:
-- 1. Find existing active rows with same business key (appt_i + dept_i)
-- 2. If data changed, expire existing row by setting expy_d = yesterday
-- 3. Insert new row with efft_d = today and expy_d = 9999-12-31
-- 4. This maintains complete audit trail of all changes over time
-- ===================================================================================== 