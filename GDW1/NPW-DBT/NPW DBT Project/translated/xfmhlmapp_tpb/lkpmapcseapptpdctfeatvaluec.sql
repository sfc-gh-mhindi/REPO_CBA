{{ config(materialized='view', tags=['XfmHlmapp_Tpb']) }}

WITH LkpMapCseApptPDctFeatValueC AS (
	SELECT
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.APPT_PDCT_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.FEAT_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.SRCE_SYST_APPT_FEAT_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.EFFT_D,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.SRCE_SYST_C,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.SRCE_SYST_APPT_OVRD_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.OVRD_FEAT_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.SRCE_SYST_STND_VALU_Q,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.SRCE_SYST_STND_VALU_R,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.SRCE_SYST_STND_VALU_A,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.CNCY_C,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.ACTL_VALU_Q,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.ACTL_VALU_R,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.ACTL_VALU_A,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.FEAT_SEQN_N,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.FEAT_STRT_D,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.FEE_CHRG_D,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.OVRD_REAS_C,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.FEE_ADD_TO_TOTL_F,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.FEE_CAPL_F,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.EXPY_D,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.EROR_SEQN_I,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.ROW_SECU_ACCS_C,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.PEXA_FLAG,
		{{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.FEAT_VALU_C,
		{{ ref('XfmTrans__OutCseApptPdctFeat') }}.RUN_STRM
	FROM {{ ref('XfmTrans__OutCseApptPdctFeat') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_FEAT') }} ON {{ ref('XfmTrans__OutCseApptPdctFeat') }}.PEXA_FLAG = {{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.PEXA_FLAG
)

SELECT * FROM LkpMapCseApptPDctFeatValueC