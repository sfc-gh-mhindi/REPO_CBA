{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH CpyRename AS (
	SELECT
		{{ ref('SrcApptPdctFnddInssPremapDS') }}.SUBTYPE_CODE AS SBTY_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		{{ ref('SrcApptPdctFnddInssPremapDS') }}.TU_APP_CONDITION_CAT_ID AS COND_APPT_CAT_ID,
		CONDITION_MET_DATE,
		ORIG_ETL_D
	FROM {{ ref('SrcApptPdctFnddInssPremapDS') }}
)

SELECT * FROM CpyRename