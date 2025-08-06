{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH Lookup_239 AS (
	SELECT
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.APPT_I,
		{{ ref('LkpMAP_CSE_APPT_QSTN_RESP_HLLks') }}.QSTN_C,
		{{ ref('Copy_248') }}.RESP_C AS QSTN_C_EXT,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.EFFT_D,
		{{ ref('Copy_248') }}.RESP_C,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.RESP_CMMT_X,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.EXPY_D,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.PATY_I,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.ROW_SECU_ACCS_C,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.EROR_SEQN_I,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.RUN_STRM,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.ANS_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.QSTN_ID
	FROM {{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}
	LEFT JOIN {{ ref('Copy_248') }} ON {{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.ANS_ID = {{ ref('Copy_248') }}.QA_ANSWER_ID_2
	LEFT JOIN {{ ref('Copy_248') }} ON {{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.ANS_ID = {{ ref('Copy_248') }}.QA_ANSWER_ID_2
	LEFT JOIN {{ ref('LkpMAP_CSE_APPT_QSTN_RESP_HLLks') }} ON {{ ref('XfmCheckInApptPdctFnddInssdNulls__DSLink238') }}.QSTN_ID = {{ ref('LkpMAP_CSE_APPT_QSTN_RESP_HLLks') }}.QA_QUESTION_ID
)

SELECT * FROM Lookup_239