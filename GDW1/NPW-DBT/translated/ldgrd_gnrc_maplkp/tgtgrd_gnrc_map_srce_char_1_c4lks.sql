{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__srce__char__1__c2', incremental_strategy='insert_overwrite', tags=['LdGRD_GNRC_MAPLkp']) }}

SELECT
	MAP_TYPE_C,
	SRCE_CHAR_1_C,
	TARG_CHAR_C 
FROM {{ ref('Copy_63') }}