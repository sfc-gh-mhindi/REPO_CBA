{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_grd__gnrc__map__map__type__c', incremental_strategy='insert_overwrite', tags=['LdGRD_GNRC_MAPLkpDS']) }}

SELECT
	MAP_TYPE_C,
	SRCE_NUMC_1_C,
	SRCE_CHAR_1_C,
	SRCE_NUMC_2_C,
	SRCE_CHAR_2_C,
	SRCE_NUMC_3_C,
	SRCE_CHAR_3_C,
	SRCE_NUMC_4_C,
	SRCE_CHAR_4_C,
	SRCE_NUMC_5_C,
	SRCE_CHAR_5_C,
	SRCE_NUMC_6_C,
	SRCE_CHAR_6_C,
	SRCE_NUMC_7_C,
	SRCE_CHAR_7_C,
	TARG_NUMC_C,
	TARG_CHAR_C 
FROM {{ ref('XfmConversions') }}