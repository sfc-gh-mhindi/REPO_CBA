{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH 
appt_gnrc_date AS (
	SELECT
	*
	FROM {{ ref("appt_gnrc_date")  }}),
ApptGnrcDate AS (SELECT APPT_I, DATE_ROLE_C, GNRC_ROLE_D, CHNG_REAS_TYPE_C, 1 AS dummy FROM APPT_GNRC_DATE WHERE EXPY_D = '9999-12-31')


SELECT * FROM ApptGnrcDate