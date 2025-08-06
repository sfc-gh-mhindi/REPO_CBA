{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__com__bus__sm__case__state__20060616__maptargnumcc', incremental_strategy='insert_overwrite', tags=['XfmSmCaseStateFrmExt']) }}

{
            dbt_utils.union_relations(
                relations=[ref('ErrMapCseSmCaseStusSeq'), ref('ErrStartDtSeq'), ref('ErrEndDtSeq')]
            )
        }