{{ config(materialized='view', tags=['XfmCclAppProdFrmExt2']) }}

WITH CpyRenameGRD_GNRC_MAPLks AS (
	SELECT
		MAP_TYPE_C,
		SRCE_NUMC_1_C,
		SRCE_NUMC_2_C,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C AS FEAT_I_1,
		SRCE_CHAR_1_C,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C AS FEAT_I_2,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C AS FEAT_I_3,
		SRCE_CHAR_2_C,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C AS FEAT_I_4,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C AS FEAT_I_5,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C AS FEAT_I_6
	FROM {{ ref('SrcGRD_GNRC_MAPLks') }}
)

SELECT * FROM CpyRenameGRD_GNRC_MAPLks