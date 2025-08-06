{{ config(materialized='view', tags=['XfmAntn']) }}

WITH 
tmp_rm_rate_cmmt AS (
	SELECT
	*
	FROM {{ ref("tmp_rm_rate_cmmt")  }}),
antn AS (
	SELECT
	*
	FROM {{ ref("antn")  }}),
ANTN AS (SELECT TGT.ANTN_I, 1 AS dummy FROM ANTN INNER JOIN TMP_RM_RATE_CMMT ON TGT.ANTN_I = TMP.ANTN_I WHERE TGT.ANTN_TYPE_C = 'RMCO' AND TGT.EXPY_D = '9999-12-31')


SELECT * FROM ANTN