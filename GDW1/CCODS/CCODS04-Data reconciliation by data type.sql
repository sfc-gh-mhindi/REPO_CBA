-- =====================================================================================
-- PLAN_BALN_SEGM_MSTR_NPW Data Comparison - Segregated by Data Type
-- =====================================================================================
-- Purpose: Compare data between plan_baln_segm_mstr_npw and plan_baln_segm_mstr
-- Segregated by data type to make differences easier to identify and troubleshoot
-- =====================================================================================

-- =====================================================================================
-- 1. PRIMARY KEY & IDENTIFIER COLUMNS (STRING)
-- =====================================================================================
-- Key columns that identify unique records
SELECT 'PRIMARY_KEYS_IDENTIFIERS' as data_category, *
FROM (
    SELECT
        acct_i,
        corp_idnn,
        plan_idnn,
        plan_sqno,
        srce_syst_acct_numb,
        stor_idnn
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        trim(corp_idnn),
        trim(plan_idnn),
        plan_sqno,
        trim(srce_syst_acct_numb),
        trim(stor_idnn)
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 2. STATUS & CODE COLUMNS (STRING)
-- =====================================================================================
-- Status and code fields that control business logic
SELECT 'STATUS_CODE_FIELDS' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        orig_stat,
        plan_type,
        plan_catg,
        post_pur_stus,
        term_stus,
        int_defr_stus,
        payt_defr_stus,
        last_rate_code,
        last_min_payt_code,
        user_code,
        migr_to_plan_idnn,
        sche_payf_reas
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        trim(orig_stat),
        trim(plan_type),
        trim(plan_catg),
        trim(post_pur_stus),
        trim(term_stus),
        trim(int_defr_stus),
        trim(payt_defr_stus),
        trim(last_rate_code),
        trim(last_min_payt_code),
        trim(user_code),
        trim(migr_to_plan_idnn),
        trim(sche_payf_reas)
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 3. COUNTER & SEQUENCE COLUMNS (DECIMAL - Small Numbers)
-- =====================================================================================
-- Counters, sequences, and small numeric values
SELECT 'COUNTERS_SEQUENCES' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        time_bill,
        spec_term_cycl,
        int_defr_cycl,
        payt_defr_cycl,
        fix_pacy,
        last_rate_code_seqn,
        last_min_payt_code_seqn,
        migr_to_plan_sqno,
        frnt_load_ismt_bill,
        dlay_day_qnty,
        int_defr_day_qnty,
        ismt_qnty,
        ismt_prev_qnty,
        dspt_qnty
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        time_bill,
        spec_term_cycl,
        int_defr_cycl,
        payt_defr_cycl,
        fix_pacy,
        last_rate_code_seqn,
        last_min_payt_code_seqn,
        migr_to_plan_sqno,
        frnt_load_ismt_bill,
        dlay_day_qnty,
        int_defr_day_qnty,
        ismt_qnty,
        ismt_prev_qnty,
        dspt_qnty
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 4. DATE COLUMNS
-- =====================================================================================
-- All date fields for temporal analysis
SELECT 'DATE_FIELDS' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        date_spec_term_end,
        date_int_defr_end,
        date_payt_defr_end,
        date_frst_tran,
        date_paid_off,
        date_last_payt,
        date_last_mntn,
        plan_due_date,
        int_fee_end_date,
        date_migr,
        frnt_load_last_ismt_date,
        sche_payf_date,
        actl_payf_date,
        date_ismt_term_chng,
        date_loan_paid_out,
        prjc_pay_off_date,
        dspt_old_date
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        date_spec_term_end,
        date_int_defr_end,
        date_payt_defr_end,
        date_frst_tran,
        date_paid_off,
        date_last_payt,
        date_last_mntn,
        plan_due_date,
        int_fee_end_date,
        date_migr,
        frnt_load_last_ismt_date,
        sche_payf_date,
        actl_payf_date,
        date_ismt_term_chng,
        date_loan_paid_out,
        prjc_pay_off_date,
        dspt_old_date
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 5. BALANCE AMOUNTS (DECIMAL 13,2) - High Value Financial Fields
-- =====================================================================================
-- Primary balance and amount fields
SELECT 'BALANCE_AMOUNTS_PRIMARY' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        lftd_high_baln,
        payt_tabl_high_baln,
        lftd_ichg,
        lftd_int_wavr,
        fix_payt_amt,
        last_min_payt_amt,
        curr_min_payt_due,
        min_payt_past_due,
        last_appy_payt_amt,
        payt_bfor_grce,
        orig_baln,
        int_free_baln,
        open_baln_curr_cycl,
        orig_loan_baln,
        lftd_all_cr,
        lftd_int_save,
        prev_cycl_int_save,
        prev_min_payt_amt
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        lftd_high_baln,
        payt_tabl_high_baln,
        lftd_ichg,
        lftd_int_wavr,
        fix_payt_amt,
        last_min_payt_amt,
        curr_min_payt_due,
        min_payt_past_due,
        last_appy_payt_amt,
        payt_bfor_grce,
        orig_baln,
        int_free_baln,
        open_baln_curr_cycl,
        orig_loan_baln,
        lftd_all_cr,
        lftd_int_save,
        prev_cycl_int_save,
        prev_min_payt_amt
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 6. TRANSACTION AMOUNTS (DECIMAL 13,2) - CTD & Transaction Fields
-- =====================================================================================
-- Cycle-to-date and transaction amounts
SELECT 'TRANSACTION_AMOUNTS' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        ctd_sale_tran,
        ctd_dr_tran,
        ctd_cr_tran,
        ctd_payt_tran,
        ctd_csad_fee_amt,
        ctd_syst_genr_csad_fee_amt,
        int_defr_amt,
        dspt_amt,
        excl_dspt_amt,
        acrl_dspt_amt,
        sche_payf_amt,
        sche_payf_fee,
        actl_payf_amt,
        frnt_load_insr_paid,
        frnt_load_rebt_insr_paid
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        ctd_sale_tran,
        ctd_dr_tran,
        ctd_cr_tran,
        ctd_payt_tran,
        ctd_csad_fee_amt,
        ctd_syst_genr_csad_fee_amt,
        int_defr_amt,
        dspt_amt,
        excl_dspt_amt,
        acrl_dspt_amt,
        sche_payf_amt,
        sche_payf_fee,
        actl_payf_amt,
        frnt_load_insr_paid,
        frnt_load_rebt_insr_paid
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 7. UNPAID BALANCE CATEGORIES (DECIMAL 13,2) - Interest, Fees, Principal
-- =====================================================================================
-- Unpaid balances by category (Interest, Fees, AMF, Insurance, Principal)
SELECT 'UNPAID_BALANCES_INTEREST_FEES' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        -- Interest Unpaid Balances
        int_unpd_open_not_acrl_baln,
        int_unpd_ctd_not_acrl_baln,
        int_unpd_curr_cycl_baln,
        int_unpd_1_cycl_ago_baln,
        int_unpd_2_cycl_ago_baln,
        -- Fee Unpaid Balances
        fee_unpd_open_not_acrl_baln,
        fee_unpd_ctd_not_acrl_baln,
        fee_unpd_curr_cycl_baln,
        fee_unpd_1_cycl_ago_baln,
        fee_unpd_2_cycl_ago_baln,
        -- AMF Unpaid Balances
        amf_unpd_open_not_acrl_baln,
        amf_unpd_ctd_not_acrl_baln,
        amf_unpd_curr_cycl_baln,
        amf_unpd_1_cycl_ago_baln,
        amf_unpd_2_cycl_ago_baln
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        int_unpd_open_not_acrl_baln,
        int_unpd_ctd_not_acrl_baln,
        int_unpd_curr_cycl_baln,
        int_unpd_1_cycl_ago_baln,
        int_unpd_2_cycl_ago_baln,
        fee_unpd_open_not_acrl_baln,
        fee_unpd_ctd_not_acrl_baln,
        fee_unpd_curr_cycl_baln,
        fee_unpd_1_cycl_ago_baln,
        fee_unpd_2_cycl_ago_baln,
        amf_unpd_open_not_acrl_baln,
        amf_unpd_ctd_not_acrl_baln,
        amf_unpd_curr_cycl_baln,
        amf_unpd_1_cycl_ago_baln,
        amf_unpd_2_cycl_ago_baln
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 8. UNPAID BALANCES - Insurance & Principal (DECIMAL 13,2)
-- =====================================================================================
SELECT 'UNPAID_BALANCES_INSURANCE_PRINCIPAL' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        -- Insurance Unpaid Balances
        insr_unpd_open_not_acrl_baln,
        insr_unpd_ctd_not_acrl_baln,
        insr_unpd_curr_cycl_baln,
        insr_unpd_1_cycl_ago_baln,
        insr_unpd_2_cycl_ago_baln,
        -- Principal Unpaid Balances
        prin_unpd_open_not_acrl_baln,
        prin_unpd_ctd_not_acrl_baln,
        prin_unpd_curr_cycl_baln,
        prin_unpd_1_cycl_ago_baln,
        prin_unpd_2_cycl_ago_baln
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        insr_unpd_open_not_acrl_baln,
        insr_unpd_ctd_not_acrl_baln,
        insr_unpd_curr_cycl_baln,
        insr_unpd_1_cycl_ago_baln,
        insr_unpd_2_cycl_ago_baln,
        prin_unpd_open_not_acrl_baln,
        prin_unpd_ctd_not_acrl_baln,
        prin_unpd_curr_cycl_baln,
        prin_unpd_1_cycl_ago_baln,
        prin_unpd_2_cycl_ago_baln
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 9. AGGREGATE BALANCES (DECIMAL 15,2) - Larger Precision Fields
-- =====================================================================================
SELECT 'AGGREGATE_BALANCES' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        aggr_curr_cycl_baln,
        aggr_1_cycl_ago_baln,
        aggr_2_cycl_ago_baln,
        aggr_dlay_baln,
        int_defr_aggr_curr_baln
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        aggr_curr_cycl_baln,
        aggr_1_cycl_ago_baln,
        aggr_2_cycl_ago_baln,
        aggr_dlay_baln,
        int_defr_aggr_curr_baln
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 10. FRONT LOAD & GOVERNMENT CHARGES (DECIMAL 13,2)
-- =====================================================================================
SELECT 'FRONT_LOAD_GOVT_CHARGES' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        -- Front Load Fields
        frnt_load_orig_int_amt,
        frnt_load_earn_int_amt,
        frnt_load_rebt_int_amt,
        frnt_load_orig_insr_amt,
        frnt_load_earn_insr_amt,
        frnt_load_upfr_insr_amt,
        frnt_load_rebt_insr_amt,
        -- Government Charges
        govt_chrg_not_acrl_open_amt,
        govt_chrg_not_acrl_ctd_amt,
        govt_chrg_curr_cycl_amt,
        govt_chrg_1_cycl_ago_amt,
        govt_chrg_2_cycl_ago_amt
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        frnt_load_orig_int_amt,
        frnt_load_earn_int_amt,
        frnt_load_rebt_int_amt,
        frnt_load_orig_insr_amt,
        frnt_load_earn_insr_amt,
        frnt_load_upfr_insr_amt,
        frnt_load_rebt_insr_amt,
        govt_chrg_not_acrl_open_amt,
        govt_chrg_not_acrl_ctd_amt,
        govt_chrg_curr_cycl_amt,
        govt_chrg_1_cycl_ago_amt,
        govt_chrg_2_cycl_ago_amt
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- 11. RATES & PRECISION FIELDS (DECIMAL with varying precision)
-- =====================================================================================
SELECT 'RATES_PRECISION_FIELDS' as data_category, *
FROM (
    SELECT
        acct_i, -- Key for joining
        last_irte,           -- DECIMAL(7,2)
        int_save_rate,       -- DECIMAL(2,2)
        frnt_load_irte,      -- DECIMAL(6,6)
        ctd_fncl_chrg_rev,   -- DECIMAL(7,2)
        term                 -- DECIMAL(13,2)
    FROM npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
    WHERE true
    MINUS
    SELECT
        acct_i,
        last_irte,
        int_save_rate,
        frnt_load_irte,
        ctd_fncl_chrg_rev,
        term
    FROM npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
    WHERE true
) differences;

-- =====================================================================================
-- SUMMARY QUERY - Count differences by category
-- =====================================================================================
-- Uncomment to get a summary count of differences by data type category
/*
WITH all_differences AS (
    -- Union all the above queries with their category labels
    -- This would need to be manually constructed based on the results above
    SELECT 'PRIMARY_KEYS_IDENTIFIERS' as category, COUNT(*) as difference_count FROM (...) 
    UNION ALL
    SELECT 'STATUS_CODE_FIELDS' as category, COUNT(*) as difference_count FROM (...)
    -- ... continue for all categories
)
SELECT 
    category,
    difference_count,
    CASE 
        WHEN difference_count = 0 THEN '✅ MATCH'
        ELSE '❌ DIFFERENCES FOUND'
    END as status
FROM all_differences
ORDER BY difference_count DESC;
*/ 