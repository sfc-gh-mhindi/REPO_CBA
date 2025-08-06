{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH LkpMapApptAsetStloHm AS (
	SELECT
		{{ ref('XfmTrans__OutApptAset') }}.HLM_APP_ID,
		{{ ref('XfmTrans__OutApptAset') }}.ASSET_LIABILITY_ID,
		{{ ref('XfmTrans__OutApptAset') }}.APPT_I,
		{{ ref('XfmTrans__OutApptAset') }}.ASET_I,
		{{ ref('XfmTrans__OutApptAset') }}.PRIM_SECU_F,
		{{ ref('XfmTrans__OutApptAset') }}.EFFT_D,
		{{ ref('XfmTrans__OutApptAset') }}.EXPY_D,
		{{ ref('XfmTrans__OutApptAset') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans__OutApptAset') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmTrans__OutApptAset') }}.eror_seqn_i,
		{{ ref('XfmTrans__OutApptAset') }}.ASET_SETL_REQD,
		{{ ref('XfmTrans__OutApptAset') }}.RUN_STRM,
		{{ ref('XfmTrans__OutApptAset') }}.SRCE_SYST_C,
		{{ ref('XfmTrans__OutApptAset') }}.SETL_LOCN_X,
		{{ ref('XfmTrans__OutApptAset') }}.FORWARD_DOC_TO,
		{{ ref('SrcMapApptAsetStloHm') }}.FRWD_DOCU_C,
		{{ ref('XfmTrans__OutApptAset') }}.SETL_CMMT_X
	FROM {{ ref('XfmTrans__OutApptAset') }}
	LEFT JOIN {{ ref('SrcMapApptAsetStloHm') }} ON {{ ref('XfmTrans__OutApptAset') }}.FORWARD_DOC_TO = {{ ref('SrcMapApptAsetStloHm') }}.FRWD_DOCU_TO
)

SELECT * FROM LkpMapApptAsetStloHm