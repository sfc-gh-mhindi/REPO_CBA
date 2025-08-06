{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_FEATFrmStg_IntRateAmtMargin']) }}

WITH --Manual Task - PxSwitch - SwRejectFlag

SELECT * FROM SwRejectFlag