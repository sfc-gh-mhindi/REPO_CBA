{{ config(materialized='view', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c2 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c2")  }})
SrcGRD_GNRC_MAP_Lks1 AS (
	SELECT MAP_TYPE_C,
		SRCE_CHAR_1_C,
		TARG_CHAR_C
	FROM _cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c2
)

SELECT * FROM SrcGRD_GNRC_MAP_Lks1