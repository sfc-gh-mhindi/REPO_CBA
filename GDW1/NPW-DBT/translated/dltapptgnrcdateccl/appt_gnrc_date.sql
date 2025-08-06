{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH 
appt_gnrc_date AS (
	SELECT
	*
	FROM {{ ref("appt_gnrc_date")  }}),
APPT_GNRC_DATE AS (SELECT APPT_I, DATE_ROLE_C, EFFT_D, GNRC_ROLE_S, MODF_S, USER_I, CHNG_REAS_TYPE_C FROM APPT_GNRC_DATE WHERE EXPY_D = '9999-12-31')


SELECT * FROM APPT_GNRC_DATE