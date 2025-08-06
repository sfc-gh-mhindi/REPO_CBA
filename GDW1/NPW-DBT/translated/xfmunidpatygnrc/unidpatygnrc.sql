{{ config(materialized='view', tags=['XfmUnidPatyGnrc']) }}

WITH 
unid_paty_gnrc AS (
	SELECT
	*
	FROM {{ ref("unid_paty_gnrc")  }}),
UnidPatyGnrc AS (SELECT UNID_PATY_I, SRCE_SYST_PATY_I FROM UNID_PATY_GNRC WHERE UNID_PATY_I LIKE 'CSEA3%')


SELECT * FROM UnidPatyGnrc