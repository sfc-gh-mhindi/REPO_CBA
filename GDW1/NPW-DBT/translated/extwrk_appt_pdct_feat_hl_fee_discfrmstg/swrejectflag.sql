{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_FEAT_HL_FEE_DISCFrmStg']) }}

WITH --Manual Task - PxSwitch - SwRejectFlag

SELECT * FROM SwRejectFlag