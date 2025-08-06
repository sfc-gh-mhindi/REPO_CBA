{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.HL_APP_PROD_ID,
		{{ ref('TgtMAP_CSE_APPT_CONDLks') }}.APPT_COND_C AS COND_C,
		{{ ref('CpyRename') }}.CONDITION_MET_DATE AS APPT_PDCT_COND_MEET_D,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		In.SUBTYPE_CODE AS SBTY_CODE,
		In.HL_APP_PROD_ID,
		In.TU_APP_CONDITION_ID,
		In.TU_APP_CONDITION_CAT_ID AS COND_APPT_CAT_ID,
		In.CONDITION_MET_DATE,
		In.ORIG_ETL_D
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_APPT_CONDLks') }} ON 
)

SELECT * FROM LkpReferences