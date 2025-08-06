{{ config(materialized='view', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__numc__1__c__targ__char__c AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__numc__1__c__targ__char__c")  }})
SrcMAP_GRD_GNRC_MAP_FEAT_ILks AS (
	SELECT MAP_TYPE_C,
		TARG_CHAR_C,
		SRCE_NUMC_1_C,
		SRCE_NUMC_2_C
	FROM _cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__numc__1__c__targ__char__c
)

SELECT * FROM SrcMAP_GRD_GNRC_MAP_FEAT_ILks