{{ config(materialized='view', tags=['LdDltClp_App_Pdct_Feat']) }}

WITH XFR_Split__To_Tmp_Insert_USR_TEM_DS AS (
	SELECT
		-- *SRC*: \(20)If IsNull(To_Split_XFR.APPT_PDCT_I) OR IsNull(To_Split_XFR.FEAT_I) Then "F" Else "T",
		IFF({{ ref('CC_Identify_Deltas') }}.APPT_PDCT_I IS NULL OR {{ ref('CC_Identify_Deltas') }}.FEAT_I IS NULL, 'F', 'T') AS ActionRequired,
		APPT_PDCT_I,
		FEAT_I,
		SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: StringToDate(LEFT(ETL_PROCESS_DT, 4) : '-' : LEFT(RIGHT(ETL_PROCESS_DT, 4), 2) : '-' : RIGHT(ETL_PROCESS_DT, 2), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(LEFT(ETL_PROCESS_DT, 4), '-'), LEFT(RIGHT(ETL_PROCESS_DT, 4), 2)), '-'), RIGHT(ETL_PROCESS_DT, 2)), '%yyyy-%mm-%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_APPT_OVRD_I,
		-- *SRC*: SetNull(),
		SETNULL() AS OVRD_FEAT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_Q,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_R,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_A,
		-- *SRC*: SetNull(),
		SETNULL() AS CNCY_C,
		-- *SRC*: SetNull(),
		SETNULL() AS ACTL_VALU_Q,
		ACTL_VALU_R,
		-- *SRC*: SetNull(),
		SETNULL() AS ACTL_VALU_A,
		-- *SRC*: SetNull(),
		SETNULL() AS FEAT_SEQN_N,
		-- *SRC*: SetNull(),
		SETNULL() AS FEAT_STRT_D,
		-- *SRC*: SetNull(),
		SETNULL() AS FEE_CHRG_D,
		-- *SRC*: SetNull(),
		SETNULL() AS OVRD_REAS_C,
		-- *SRC*: SetNull(),
		SETNULL() AS FEE_ADD_TO_TOTL_F,
		-- *SRC*: SetNull(),
		SETNULL() AS FEE_CAPL_F,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('CC_Identify_Deltas') }}
	WHERE ActionRequired = 'T' AND {{ ref('CC_Identify_Deltas') }}.change_code = 1 OR {{ ref('CC_Identify_Deltas') }}.change_code = 3
)

SELECT * FROM XFR_Split__To_Tmp_Insert_USR_TEM_DS