{{ config(materialized='view', tags=['MergehlmappEvntDtSmy']) }}

WITH 
evnt_date_sumy AS (
	SELECT
	*
	FROM {{ source("svcseld","evnt_date_sumy")  }}),
EVNT_DATE_SUMY AS (SELECT EVNT_I, VALU_T, VALU_D FROM EVNT_DATE_SUMY WHERE DATE_ROLE_C = 'SDBK' AND expy_d = '9999-12-31')


SELECT * FROM EVNT_DATE_SUMY