{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_FEAT_RTPERCMARGFrmStg']) }}

WITH --Manual Task - PxSwitch - SwRejectFlag

SELECT * FROM SwRejectFlag