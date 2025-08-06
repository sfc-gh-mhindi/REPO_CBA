{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_grd__gnrc__map__map__type__c__srce__numc__1__c AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_grd__gnrc__map__map__type__c__srce__numc__1__c")  }})
SrcGRD_GNRC_MAP_SRCE_NUM_1_CLks AS (
	SELECT MAP_TYPE_C,
		SRCE_NUMC_1_C,
		TARG_CHAR_C
	FROM _cba__app_csel4_dev_lookupset_grd__gnrc__map__map__type__c__srce__numc__1__c
)

SELECT * FROM SrcGRD_GNRC_MAP_SRCE_NUM_1_CLks