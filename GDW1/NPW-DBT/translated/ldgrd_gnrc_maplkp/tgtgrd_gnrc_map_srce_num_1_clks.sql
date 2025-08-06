{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__map__type__c__srce__numc__1__c', incremental_strategy='insert_overwrite', tags=['LdGRD_GNRC_MAPLkp']) }}

SELECT
	MAP_TYPE_C,
	SRCE_NUMC_1_C,
	TARG_CHAR_C 
FROM {{ ref('Copy_62') }}