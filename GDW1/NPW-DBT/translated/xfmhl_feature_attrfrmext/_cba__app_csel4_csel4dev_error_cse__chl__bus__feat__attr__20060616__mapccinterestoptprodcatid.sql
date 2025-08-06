{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__chl__bus__feat__attr__20060616__mapccinterestoptprodcatid', incremental_strategy='insert_overwrite', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

{
            dbt_utils.union_relations(
                relations=[ref('ErrMapHlFeatureCatIdSeq'), ref('ErrMapCassCodeIdSeq')]
            )
        }