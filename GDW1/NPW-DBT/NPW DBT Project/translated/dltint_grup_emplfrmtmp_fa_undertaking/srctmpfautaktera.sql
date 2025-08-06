{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_UNDERTAKING']) }}

WITH 
tmp_fa_utak AS (
	SELECT
	*
	FROM {{ ref("tmp_fa_utak")  }}),
int_grup_empl AS (
	SELECT
	*
	FROM {{ ref("int_grup_empl")  }}),
SrcTmpFAUtakTera AS (SELECT a.INT_GRUP_I, a.EMPL_I, a.EMPL_ROLE_C, b.INT_GRUP_I AS OLD_INT_GRUP_I, b.EMPL_I AS OLD_EMPL_I, b.EMPL_ROLE_C AS OLD_EMPL_ROLE_C FROM TMP_FA_UTAK LEFT OUTER JOIN INT_GRUP_EMPL ON TRIM(a.INT_GRUP_I) = TRIM(b.INT_GRUP_I) AND b.EXPY_D = '9999-12-31' WHERE INT_GRUP_EMPL_F = 'Y')


SELECT * FROM SrcTmpFAUtakTera