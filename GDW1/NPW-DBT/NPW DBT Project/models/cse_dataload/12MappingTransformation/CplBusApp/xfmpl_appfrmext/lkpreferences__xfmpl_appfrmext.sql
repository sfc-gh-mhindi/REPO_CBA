WITH LkpReferences AS (
    select 
    r.PL_APP_ID,
    r.NOMINATED_BRANCH_ID,
    r.PL_PACKAGE_CAT_ID,
    r.ORIG_ETL_D,
    l.PDCT_N
    from {{ ref('cpyrename__xfmpl_appfrmext') }} r
    left join {{ ref('srcmap_cse_pack_pdct_pllks__xfmpl_appfrmext') }} l on r.PL_PACKAGE_CAT_ID=l.PL_PACKAGE_CAT_ID
)

SELECT * FROM LkpReferences