{{ config(materialized='view', tags=['XfmChlBusFeeDiscFeeFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c")  }})
SrcGRD_GNRC_MAP_SRCE_CHAR_1_CLks AS (
	SELECT MAP_TYPE_C,
		TARG_CHAR_C,
		SRCE_CHAR_1_C
	FROM _cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c
)

SELECT * FROM SrcGRD_GNRC_MAP_SRCE_CHAR_1_CLks