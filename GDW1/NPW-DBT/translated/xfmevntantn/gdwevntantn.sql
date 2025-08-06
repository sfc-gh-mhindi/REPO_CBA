{{ config(materialized='view', tags=['XfmEvntAntn']) }}

WITH 
evnt_antn AS (
	SELECT
	*
	FROM {{ ref("evnt_antn")  }}),
tmp_rm_rate_cmmt AS (
	SELECT
	*
	FROM {{ ref("tmp_rm_rate_cmmt")  }}),
GdwEvntAntn AS (SELECT TGT.EVNT_I, TGT.ANTN_I, 1 AS dummy FROM EVNT_ANTN INNER JOIN TMP_RM_RATE_CMMT ON TGT.EVNT_I = TMP.EVNT_I AND TGT.ANTN_I = TMP.ANTN_I WHERE TGT.EXPY_D = '9999-12-31')


SELECT * FROM GdwEvntAntn