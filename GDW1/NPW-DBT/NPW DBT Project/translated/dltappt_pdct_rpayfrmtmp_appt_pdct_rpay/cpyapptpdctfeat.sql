{{ config(materialized='view', tags=['DltAPPT_PDCT_RPAYFrmTMP_APPT_PDCT_RPAY']) }}

WITH CpyApptPdctFeat AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_RPAY_TYPE_C,
		NEW_PAYT_FREQ_C,
		{{ ref('SrcTmpApptPdctRpayTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctRpayTera') }}.OLD_RPAY_TYPE_C AS NEW_RPAY_TYPE_C,
		{{ ref('SrcTmpApptPdctRpayTera') }}.OLD_PAYT_FREQ_C AS NEW_PAYT_FREQ_C,
		NEW_STRT_RPAY_D,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctRpayTera') }}
)

SELECT * FROM CpyApptPdctFeat