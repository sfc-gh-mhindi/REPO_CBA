{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__srce__char__2__c', incremental_strategy='insert_overwrite', tags=['LdGRD_GNRC_MAPLkp']) }}

SELECT
	MAP_TYPE_C,
	SRCE_CHAR_1_C,
	SRCE_CHAR_2_C,
	TARG_CHAR_C 
FROM {{ ref('Copy_65') }}