{{ config(materialized='view', tags=['XfmUnidPatyNameGnrc']) }}

WITH 
unid_paty_name_gnrc AS (
	SELECT
	*
	FROM {{ ref("unid_paty_name_gnrc")  }}),
UnidPatyNameGnrc AS (SELECT UNID_PATY_I, SRCE_SYST_C FROM UNID_PATY_NAME_GNRC WHERE UNID_PATY_I LIKE 'CSEA3%')


SELECT * FROM UnidPatyNameGnrc