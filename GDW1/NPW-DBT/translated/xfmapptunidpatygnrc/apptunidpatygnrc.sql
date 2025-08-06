{{ config(materialized='view', tags=['XfmApptUnidPatyGnrc']) }}

WITH 
appt_unid_paty_gnrc AS (
	SELECT
	*
	FROM {{ source("tdcsodepo","appt_unid_paty_gnrc")  }}),
ApptUnidPatyGnrc AS (SELECT APPT_I, UNID_PATY_I, SRCE_SYST_C, EXPY_D FROM APPT_UNID_PATY_GNRC WHERE UNID_PATY_I LIKE 'CSEA3%' AND APPT_I LIKE 'CSEHL%' AND EXPY_D = '9999-12-31')


SELECT * FROM ApptUnidPatyGnrc