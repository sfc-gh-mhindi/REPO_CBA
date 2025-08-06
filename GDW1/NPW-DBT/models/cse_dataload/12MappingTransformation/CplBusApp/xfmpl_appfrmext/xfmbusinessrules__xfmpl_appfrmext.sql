with modnullhandling as (
    select 
        PL_APP_ID,
        NOMINATED_BRANCH_ID,
        PL_PACKAGE_CAT_ID,
        ORIG_ETL_D,
        PDCT_N,
        CASE 
            WHEN (length(trim(coalesce(PL_PACKAGE_CAT_ID, ''))) = 0)
                THEN NULL
            ELSE PDCT_N
        END AS svPdctN,
        CASE 
            WHEN (length(trim(coalesce(NOMINATED_BRANCH_ID, ''))) = 0)
                THEN 'N'
            ELSE 'Y'
        END AS svLoadApptDept,
        CASE 
            WHEN (length(trim(coalesce(PL_PACKAGE_CAT_ID, ''))) = 0)
                THEN 'N'
            ELSE 'Y'
        END AS svLoadApptPdct,
        CASE 
            WHEN PDCT_N = '800999'
                THEN 'RPR2104'
            ELSE ''
        END AS svErrorCode,
        CASE 
            WHEN svErrorCode IS NOT NULL
                AND svErrorCode <> ''
                THEN TRUE
            ELSE FALSE
        END AS svrejectflag
    from {{ ref('modnullhandling__xfmpl_appfrmext') }}
)

select
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    PL_APP_ID,
    ORIG_ETL_D,
    PDCT_N,
    svLoadApptDept,
    svLoadApptPdct,
    svPdctN,
    svErrorCode,
    svrejectflag
from modnullhandling