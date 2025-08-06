{{ config(materialized='view', tags=['DltAPPT_PRMO_TRAKFrmTMP_APPT_PRMO_TRAK']) }}

WITH xf_DecideRecordType__Ln_Insert_Records AS (
	SELECT
		APPT_PDCT_I,
		TRAK_I,
		'MKLI' AS TRAK_IDNN_TYPE_C,
		{{ ref('tc_ApptPrmoTrakDlta') }}.MOD_DATE AS PRVD_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd'),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('tc_ApptPrmoTrakDlta') }}
	WHERE {{ ref('tc_ApptPrmoTrakDlta') }}.RECD_IND = 'I'
)

SELECT * FROM xf_DecideRecordType__Ln_Insert_Records