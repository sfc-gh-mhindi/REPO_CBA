{{ config(materialized='view', tags=['LdREJT_APPT_PDCT_FEAT']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__rel__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__rel__nulls__rejects")  }})
SrcIdNullsDS AS (
	SELECT APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__rel__nulls__rejects
)

SELECT * FROM SrcIdNullsDS