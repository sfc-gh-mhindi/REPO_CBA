{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_FrmTMP_APPT_GNRC_DATE']) }}

WITH 
appt_gnrc_date AS (
	SELECT
	*
	FROM {{ ref("appt_gnrc_date")  }}),
APPT_GNRC_DATE AS (SELECT APPT_I, DATE_ROLE_C, EFFT_D, GNRC_ROLE_D FROM APPT_GNRC_DATE WHERE EXPY_D = '9999-12-31' AND DATE_ROLE_C = 'SDBK')


SELECT * FROM APPT_GNRC_DATE