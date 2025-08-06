{{ config(materialized='view', tags=['DltAPPT_PRMO_TRAKFrmTMP_APPT_PRMO_TRAK']) }}

WITH 
appt_prmo_trak AS (
	SELECT
	*
	FROM {{ source("__var_dbt_pgdw_tech_db__","appt_prmo_trak")  }}),
tmp_appt_prmo_trak AS (
	SELECT
	*
	FROM {{ source("__var_dbt_pgdw_stag_db__","tmp_appt_prmo_trak")  }}),
tc_ApptPrmoTrakDlta AS (SELECT SRC.APPT_PDCT_I, SRC.TRAK_I, SRC.MOD_DATE, SRC.RECD_IND, TRG.EFFT_D FROM TMP_APPT_PRMO_TRAK LEFT OUTER JOIN APPT_PRMO_TRAK ON SRC.APPT_PDCT_I = TRG.APPT_PDCT_I AND SRC.TRAK_I = TRG.TRAK_I AND TRG.EXPY_D = '9999-12-31' AND TRG.TRAK_IDNN_TYPE_C = 'MKLI' AND TRG.SRCE_SYST_C = 'CSE' WHERE (TRG.APPT_PDCT_I IS NULL AND SRC.RECD_IND = 'I') OR (NOT TRG.APPT_PDCT_I IS NULL AND SRC.RECD_IND = 'D'))


SELECT * FROM tc_ApptPrmoTrakDlta