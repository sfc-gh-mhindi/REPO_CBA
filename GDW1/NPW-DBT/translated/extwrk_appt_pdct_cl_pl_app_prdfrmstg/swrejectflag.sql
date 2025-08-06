{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_CL_PL_APP_PRDFrmStg']) }}

WITH --Manual Task - PxSwitch - SwRejectFlag

SELECT * FROM SwRejectFlag