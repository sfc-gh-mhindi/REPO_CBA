{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__coi__bus__clnt__undtak__delta__chld__rejects', incremental_strategy='insert_overwrite', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING2']) }}

SELECT
	FA_CLIENT_UNDERTAKING_ID,
	FA_UNDERTAKING_ID,
	COIN_ENTITY_ID,
	CLIENT_CORRELATION_ID,
	FA_ENTITY_CAT_ID,
	FA_CHILD_STATUS_CAT_ID,
	CLIENT_RELATIONSHIP_TYPE_ID,
	CLIENT_POSITION,
	IS_PRIMARY_FLAG,
	CIF_CODE,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckDeltaAction__OutFAclientUndertakingRejectsDS') }}