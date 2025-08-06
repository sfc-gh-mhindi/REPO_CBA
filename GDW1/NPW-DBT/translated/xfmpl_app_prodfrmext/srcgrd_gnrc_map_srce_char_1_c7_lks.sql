{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_grd__gnrc__map__srce__char__1__c7 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_grd__gnrc__map__srce__char__1__c7")  }})
SrcGRD_GNRC_MAP_SRCE_CHAR_1_C7_Lks AS (
	SELECT MAP_TYPE_C2,
		TARG_CHAR_C2,
		SRCE_CHAR_1_C2
	FROM _cba__app_csel4_dev_lookupset_grd__gnrc__map__srce__char__1__c7
)

SELECT * FROM SrcGRD_GNRC_MAP_SRCE_CHAR_1_C7_Lks