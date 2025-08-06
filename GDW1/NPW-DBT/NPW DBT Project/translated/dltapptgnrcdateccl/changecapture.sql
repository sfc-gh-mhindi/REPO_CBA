{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('CopyFil') }}.APPT_I, {{ ref('Copy') }}.APPT_I) AS APPT_I,
		COALESCE({{ ref('CopyFil') }}.DATE_ROLE_C, {{ ref('Copy') }}.DATE_ROLE_C) AS DATE_ROLE_C,
		COALESCE({{ ref('CopyFil') }}.GNRC_ROLE_S, {{ ref('Copy') }}.GNRC_ROLE_S) AS GNRC_ROLE_S,
		COALESCE({{ ref('CopyFil') }}.MODF_S, {{ ref('Copy') }}.MODF_S) AS MODF_S,
		COALESCE({{ ref('CopyFil') }}.USER_I, {{ ref('Copy') }}.USER_I) AS USER_I,
		COALESCE({{ ref('CopyFil') }}.CHNG_REAS_TYPE_C, {{ ref('Copy') }}.CHNG_REAS_TYPE_C) AS CHNG_REAS_TYPE_C,
		CASE
			WHEN {{ ref('Copy') }}.APPT_I IS NULL AND {{ ref('Copy') }}.DATE_ROLE_C IS NULL THEN '1'
			WHEN {{ ref('CopyFil') }}.APPT_I IS NULL AND {{ ref('CopyFil') }}.DATE_ROLE_C IS NULL THEN '2'
			WHEN ({{ ref('Copy') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I AND {{ ref('Copy') }}.DATE_ROLE_C = {{ ref('CopyFil') }}.DATE_ROLE_C) AND ({{ ref('Copy') }}.GNRC_ROLE_S <> {{ ref('CopyFil') }}.GNRC_ROLE_S OR {{ ref('Copy') }}.MODF_S <> {{ ref('CopyFil') }}.MODF_S OR {{ ref('Copy') }}.USER_I <> {{ ref('CopyFil') }}.USER_I OR {{ ref('Copy') }}.CHNG_REAS_TYPE_C <> {{ ref('CopyFil') }}.CHNG_REAS_TYPE_C) THEN '3'
			WHEN {{ ref('Copy') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I AND {{ ref('Copy') }}.DATE_ROLE_C = {{ ref('CopyFil') }}.DATE_ROLE_C AND {{ ref('Copy') }}.GNRC_ROLE_S = {{ ref('CopyFil') }}.GNRC_ROLE_S AND {{ ref('Copy') }}.MODF_S = {{ ref('CopyFil') }}.MODF_S AND {{ ref('Copy') }}.USER_I = {{ ref('CopyFil') }}.USER_I AND {{ ref('Copy') }}.CHNG_REAS_TYPE_C = {{ ref('CopyFil') }}.CHNG_REAS_TYPE_C THEN '0'
		END AS change_code 
	FROM {{ ref('Copy') }} 
	FULL OUTER JOIN {{ ref('CopyFil') }} 
	ON {{ ref('Copy') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I
	AND {{ ref('Copy') }}.DATE_ROLE_C = {{ ref('CopyFil') }}.DATE_ROLE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture