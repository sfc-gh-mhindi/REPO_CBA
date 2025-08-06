{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH OutRejApptQfy__rej_qlf AS (
	SELECT
		SBTY_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		COND_APPT_CAT_ID,
		-- *SRC*: trim(( IF IsNotNull((qlf_c_rej.CONDITION_MET_DATE)) THEN (qlf_c_rej.CONDITION_MET_DATE) ELSE "")),
		TRIM(IFF({{ ref('LkpReferences') }}.CONDITION_MET_DATE IS NOT NULL, {{ ref('LkpReferences') }}.CONDITION_MET_DATE, '')) AS CONDITION_MET_DATE,
		ETL_PROCESS_DT AS ETL_D,
		-- *SRC*: trim(( IF IsNotNull((qlf_c_rej.ORIG_ETL_D)) THEN (qlf_c_rej.ORIG_ETL_D) ELSE "")),
		TRIM(IFF({{ ref('LkpReferences') }}.ORIG_ETL_D IS NOT NULL, {{ ref('LkpReferences') }}.ORIG_ETL_D, '')) AS ORIG_ETL_D,
		'RPR7003' AS EROR_C
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM OutRejApptQfy__rej_qlf