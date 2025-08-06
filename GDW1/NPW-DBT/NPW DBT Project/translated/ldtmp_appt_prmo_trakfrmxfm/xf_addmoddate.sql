{{ config(materialized='view', tags=['LdTMP_APPT_PRMO_TRAKFrmXfm']) }}

WITH xf_AddModDate AS (
	SELECT
		-- *SRC*: \(20)If IsNull(Ln_Read_Delete_Recds.DELETED_KEY_1_VALUE) Then 'N' Else 'Y',
		IFF({{ ref('ds_CclAppProdTrak') }}.DELETED_KEY_1_VALUE IS NULL, 'N', 'Y') AS svApptPdctId,
		-- *SRC*: \(20)If IsNull(Ln_Read_Delete_Recds.DELETED_KEY_2_VALUE) Then 'N' Else 'Y',
		IFF({{ ref('ds_CclAppProdTrak') }}.DELETED_KEY_2_VALUE IS NULL, 'N', 'Y') AS svTrakId,
		-- *SRC*: 'CSECL' : ( IF IsNotNull((Ln_Read_Delete_Recds.DELETED_KEY_1_VALUE)) THEN (Ln_Read_Delete_Recds.DELETED_KEY_1_VALUE) ELSE ""),
		CONCAT('CSECL', IFF({{ ref('ds_CclAppProdTrak') }}.DELETED_KEY_1_VALUE IS NOT NULL, {{ ref('ds_CclAppProdTrak') }}.DELETED_KEY_1_VALUE, '')) AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((Ln_Read_Delete_Recds.DELETED_KEY_2_VALUE)) THEN (Ln_Read_Delete_Recds.DELETED_KEY_2_VALUE) ELSE ""),
		IFF({{ ref('ds_CclAppProdTrak') }}.DELETED_KEY_2_VALUE IS NOT NULL, {{ ref('ds_CclAppProdTrak') }}.DELETED_KEY_2_VALUE, '') AS TRAK_I,
		'9999-12-31' AS MOD_DATE,
		'D' AS RECD_IND
	FROM {{ ref('ds_CclAppProdTrak') }}
	WHERE svApptPdctId = 'Y' AND svTrakId = 'Y'
)

SELECT * FROM xf_AddModDate