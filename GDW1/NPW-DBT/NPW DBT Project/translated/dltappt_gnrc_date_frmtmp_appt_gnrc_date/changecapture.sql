{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_FrmTMP_APPT_GNRC_DATE']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('CopyFil') }}.APPT_I, {{ ref('Copy') }}.APPT_I) AS APPT_I,
		COALESCE({{ ref('CopyFil') }}.GNRC_ROLE_D, {{ ref('Copy') }}.GNRC_ROLE_D) AS GNRC_ROLE_D,
		CASE
			WHEN {{ ref('Copy') }}.APPT_I IS NULL THEN '1'
			WHEN {{ ref('CopyFil') }}.APPT_I IS NULL THEN '2'
			WHEN ({{ ref('Copy') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I) AND ({{ ref('Copy') }}.GNRC_ROLE_D <> {{ ref('CopyFil') }}.GNRC_ROLE_D) THEN '3'
			WHEN {{ ref('Copy') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I AND {{ ref('Copy') }}.GNRC_ROLE_D = {{ ref('CopyFil') }}.GNRC_ROLE_D THEN '0'
		END AS change_code 
	FROM {{ ref('Copy') }} 
	FULL OUTER JOIN {{ ref('CopyFil') }} 
	ON {{ ref('Copy') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture