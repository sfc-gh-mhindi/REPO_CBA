{{ config(materialized='incremental', alias='_cba__app_csel4_dev_error_cse__com__bus__ccl__chl__com__app__20120427__mapcseapptform__cclapp', incremental_strategy='insert_overwrite', tags=['XfmAppCclAppChlAppFrmExt']) }}

{
            dbt_utils.union_relations(
                relations=[ref('ErrMAP_CSE_APPT_FORMSeq'), ref('Copy_of_ErrMAP_CSE_APPT_FORMSeq')]
            )
        }