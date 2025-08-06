{{ config(materialized='view', tags=['XfmUnidPatyIdnnGnrc']) }}

WITH 
unid_paty_idnn_gnrc AS (
	SELECT
	*
	FROM {{ ref("unid_paty_idnn_gnrc")  }}),
UnidPatyIdnnGnrc AS (SELECT UNID_PATY_I, CAST(IDNN_TYPE_C AS CHAR(3)), SRCE_SYST_C FROM UNID_PATY_IDNN_GNRC WHERE UNID_PATY_I LIKE 'CSEA3%')


SELECT * FROM UnidPatyIdnnGnrc