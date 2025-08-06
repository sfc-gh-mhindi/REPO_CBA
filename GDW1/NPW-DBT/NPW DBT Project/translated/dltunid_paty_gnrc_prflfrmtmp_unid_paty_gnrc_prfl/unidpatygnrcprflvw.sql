{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH 
unid_paty_gnrc_prfl AS (
	SELECT
	*
	FROM {{ ref("unid_paty_gnrc_prfl")  }}),
UnidPatyGnrcPrflVW AS (SELECT UNID_PATY_I, GRDE_C, SUB_GRDE_C, PRNT_PRVG_F, EFFT_D FROM UNID_PATY_GNRC_PRFL WHERE EXPY_D = '9999-12-31' AND SRCE_SYST_C = 'CSE')


SELECT * FROM UnidPatyGnrcPrflVW