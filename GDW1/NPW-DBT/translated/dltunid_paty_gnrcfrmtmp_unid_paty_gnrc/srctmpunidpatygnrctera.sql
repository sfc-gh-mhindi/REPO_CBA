{{ config(materialized='view', tags=['DltUNID_PATY_GNRCFrmTMP_UNID_PATY_GNRC']) }}

WITH 
unid_paty_gnrc AS (
	SELECT
	*
	FROM {{ ref("unid_paty_gnrc")  }}),
tmp_unid_paty_gnrc AS (
	SELECT
	*
	FROM {{ ref("tmp_unid_paty_gnrc")  }}),
SrcTmpUnidPatyGnrcTera AS (SELECT A.UNID_PATY_I AS NEW_UNID_PATY_I, A.SRCE_SYST_PATY_I AS NEW_SRCE_SYST_PATY_I, B.UNID_PATY_I AS OLD_UNID_PATY_I FROM TMP_UNID_PATY_GNRC LEFT OUTER JOIN UNID_PATY_GNRC ON a.UNID_PATY_I = b.UNID_PATY_I AND b.SRCE_SYST_C = 'CSE' AND b.PATY_ROLE_C = 'TPBR' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpUnidPatyGnrcTera