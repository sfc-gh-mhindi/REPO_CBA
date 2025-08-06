{{ config(materialized='view', tags=['LdDltAPP_ANS_CTCT_EVNT_SURV']) }}

WITH Copy AS (
	SELECT
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_QSTN_C AS QSTN_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_RESP_C AS RESP_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_RESP_CMMT_X AS RESP_CMMT_X,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD__QSTN_C AS QSTN_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_RESP_C AS RESP_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_RESP_CMMT_X AS RESP_CMMT_X
	FROM {{ ref('Src_Tera_APPT_PDCT_COND') }}
)

SELECT * FROM Copy