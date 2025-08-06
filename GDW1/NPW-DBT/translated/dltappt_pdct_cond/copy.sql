{{ config(materialized='view', tags=['DltAPPT_PDCT_COND']) }}

WITH Copy AS (
	SELECT
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_COND_C AS COND_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_APPT_PDCT_COND_MEET_D AS APPT_PDCT_COND_MEET_D,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_COND_C AS COND_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_APPT_PDCT_COND_MEET_D AS APPT_PDCT_COND_MEET_D
	FROM {{ ref('Src_Tera_APPT_PDCT_COND') }}
)

SELECT * FROM Copy