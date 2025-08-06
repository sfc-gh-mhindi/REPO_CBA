{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH Transformer_32__tofile AS (
	SELECT
		-- *SRC*: \(20)If IsNull(totrans.CHNG_REAS_TYPE_C) OR Trim(( IF IsNotNull((totrans.CHNG_REAS_TYPE_C)) THEN (totrans.CHNG_REAS_TYPE_C) ELSE "")) = '' Then 'N' ELSE 'Y',
		IFF({{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C IS NULL OR TRIM(IFF({{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C IS NOT NULL, {{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C, '')) = '', 'N', 'Y') AS svChngReasType,
		-- *SRC*: \(20)IF IsNull(totrans.MODF_D) then 'N' Else  If (totrans.MODF_D = StringToDate(1111 : '-' : 11 : '-' : 11, "%yyyy-%mm-%dd")) THEN "Y" ELSE "N",
		IFF({{ ref('FunnelTrans') }}.MODF_D IS NULL, 'N', IFF({{ ref('FunnelTrans') }}.MODF_D = STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(1111, '-'), 11), '-'), 11), '%yyyy-%mm-%dd'), 'Y', 'N')) AS svIserrModfD,
		-- *SRC*: \(20)IF IsNull(totrans.MODF_T) then 'N' Else  If (totrans.MODF_T = '00:00:00') THEN "Y" ELSE "N",
		IFF({{ ref('FunnelTrans') }}.MODF_T IS NULL, 'N', IFF({{ ref('FunnelTrans') }}.MODF_T = '00:00:00', 'Y', 'N')) AS svIserrModfT,
		-- *SRC*: \(20)IF IsNull(totrans.GNRC_ROLE_S) then 'N' Else  If (totrans.GNRC_ROLE_S = '1111-11-11 00:00:00') THEN "Y" ELSE "N",
		IFF({{ ref('FunnelTrans') }}.GNRC_ROLE_S IS NULL, 'N', IFF({{ ref('FunnelTrans') }}.GNRC_ROLE_S = '1111-11-11 00:00:00', 'Y', 'N')) AS svIserrGnrcRoleS,
		APPT_I,
		DATE_ROLE_C,
		MODF_S,
		MODF_D,
		MODF_T,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		-- *SRC*: SetNull(),
		SETNULL() AS GNRC_ROLE_T,
		USER_I,
		-- *SRC*: \(20)IF svChngReasType = 'Y' THEN TRIM(totrans.CHNG_REAS_TYPE_C) ELSE '9999',
		IFF(svChngReasType = 'Y', TRIM({{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C), '9999') AS CHNG_REAS_TYPE_C,
		promise_type
	FROM {{ ref('FunnelTrans') }}
	WHERE 
)

SELECT * FROM Transformer_32__tofile