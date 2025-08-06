{{ config(materialized='view', tags=['LdApptPreApprParmIns']) }}

WITH 
appt_pre_appr_parm AS (
	SELECT
	*
	FROM {{ ref("appt_pre_appr_parm")  }}),
SrcTmpApptEvntGrupTera AS (SELECT APPT_I, APPT_PRE_APPR_PARM_I FROM APPT_PRE_APPR_PARM)


SELECT * FROM SrcTmpApptEvntGrupTera