{{ config(materialized='view', tags=['DltAPPT_PDCT_COND']) }}

WITH CC_Identify_Deltas AS (
	SELECT
		COALESCE({{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I, {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I) AS APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.COND_C, {{ ref('{{ ref('Copy') }}') }}.COND_C) AS COND_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_COND_MEET_D, {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_COND_MEET_D) AS APPT_PDCT_COND_MEET_D,
		CASE
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.COND_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.COND_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I AND {{ ref('{{ ref('Copy') }}') }}.COND_C = {{ ref('{{ ref('Copy') }}') }}.COND_C) AND ({{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_COND_MEET_D <> {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_COND_MEET_D) THEN '3'
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I AND {{ ref('{{ ref('Copy') }}') }}.COND_C = {{ ref('{{ ref('Copy') }}') }}.COND_C AND {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_COND_MEET_D = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_COND_MEET_D THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('Copy') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('Copy') }}') }} 
	ON {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I
	AND {{ ref('{{ ref('Copy') }}') }}.COND_C = {{ ref('{{ ref('Copy') }}') }}.COND_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM CC_Identify_Deltas