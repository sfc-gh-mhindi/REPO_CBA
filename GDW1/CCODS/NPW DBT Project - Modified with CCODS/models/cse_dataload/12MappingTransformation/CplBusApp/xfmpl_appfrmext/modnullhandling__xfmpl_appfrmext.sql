WITH modnullhandling AS (
select
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    PL_APP_ID,
    ORIG_ETL_D,
    coalesce(PDCT_N, 800999) as PDCT_N
from {{ ref('lkpreferences__xfmpl_appfrmext') }}
)

SELECT * FROM modnullhandling