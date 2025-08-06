{{ config(materialized='view', tags=['DltApptFrmTMP']) }}

WITH 
appt AS (
	SELECT
	*
	FROM {{ ref("appt")  }}),
tmp_appt AS (
	SELECT
	*
	FROM {{ ref("tmp_appt")  }}),
GdwAppt AS (SELECT TGT.APPT_I, 1 AS dummy FROM APPT INNER JOIN TMP_APPT ON TGT.APPT_I = TMP.APPT_I WHERE TGT.SRCE_SYST_C = 'CSE')


SELECT * FROM GdwAppt