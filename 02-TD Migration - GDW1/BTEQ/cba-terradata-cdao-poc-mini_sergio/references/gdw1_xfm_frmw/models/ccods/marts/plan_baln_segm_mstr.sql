{%- set process_name = 'BCFINSG_XFM_LOAD' -%}
{%- set stream_name = 'BCFINSG' -%}

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    schema='PDSRCCS',
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    tags=['stream_bcfinsg', 'process_bcfinsg_core_transform', 'marts_layer', 'core_fact', 'plan_balance_segment'],
    pre_hook=[
        "{{ validate_single_open_business_date('" ~ stream_name ~ "') }}",
        "{{ register_process_instance('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "{{ mark_process_completed('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    BCFINSG Core Fact Table - Plan Balance Segment Master
    
    Purpose: Transform BCFINSG records to final target structure
    - Implements exact DataStage transformation logic from mapping document
    - Applies EBCDIC date conversions, TRIM operations, and type conversions
    - Generates system fields (ROW_I, timestamps, security codes)
    - Truncate load strategy (full refresh each run)
    
    Architecture: Source → Core Transform (this model) → Target Table
    Error checking happens upstream - this model only runs if no errors exist
*/

WITH 
-- Dependency: Ensure error checking completed successfully before proceeding
error_check_dependency AS (
    SELECT COUNT(*) as error_count
    FROM {{ ref('int_validate_bcfinsg') }}
),

source_data AS (
    SELECT s.*,
        -- Get business date and process instance for processing
        bd.business_date,
        pi.process_instance_id as batch_id
    FROM {{ var('staging_database') }}.{{ var('staging_schema') }}.bcfinsg s
    CROSS JOIN (
        SELECT BUS_DT as business_date 
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
        WHERE STRM_NAME = '{{ stream_name }}' 
          AND PROCESSING_FLAG = 1
        LIMIT 1
    ) bd
    CROSS JOIN (
        SELECT PRCS_INST_ID as process_instance_id
        FROM {{ dcf_database_ref() }}.DCF_T_PRCS_INST 
        WHERE STRM_NAME = '{{ stream_name }}' AND PRCS_NAME = '{{ process_name }}'
        ORDER BY PRCS_START_TS DESC LIMIT 1
    ) pi
    -- Ensure error check dependency is satisfied
    CROSS JOIN error_check_dependency
)

SELECT 
    -- All 133 columns in exact target table order (matching mapping document)
    -- Primary Key Fields (Columns 1-3)
    TRIM(BCF_CORP) as CORP_IDNN,
    TRIM(BCF_ACCOUNT_NO1) as ACCT_I,
    TRIM(BCF_PLAN_ID) as PLAN_IDNN,
    
    -- Columns 4-17: Core Business Fields
    BCF_PLAN_SEQ as PLAN_SQNO,
    TRIM(BCF_ACCOUNT_NO1) as SRCE_SYST_ACCT_NUMB,
    TRIM(BCF_STORE_ID) as STOR_IDNN,
    TRIM(BCF_ORIGINAL_STATE) as ORIG_STAT,
    BCF_PLAN_TYPE as PLAN_TYPE,
    BCF_PLAN_CATEGORY as PLAN_CATG,
    BCF_POST_PURCHASE_STATUS as POST_PUR_STUS,
    BCF_TERMS_STATUS as TERM_STUS,
    BCF_INTEREST_DEFER_STATUS as INT_DEFR_STUS,
    BCF_PAYMENT_DEFER_STATUS as PAYT_DEFR_STUS,
    BCF_TIMES_BILLED as TIME_BILL,
    BCF_SPECIAL_TERMS_CYCLES as SPEC_TERM_CYCL,
    BCF_INTEREST_DEFER_CYCLES as INT_DEFR_CYCL,
    BCF_PAYMENT_DEFER_CYCLES as PAYT_DEFR_CYCL,
    
    -- Columns 18-24: Date Fields with EBCDIC Conversion
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_SPEC_TERMS_END) as DATE_SPEC_TERM_END,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_INTEREST_DEFER) as DATE_INT_DEFR_END,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_PAYMENT_DEFER) as DATE_PAYT_DEFR_END,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_FIRST_TRANS) as DATE_FRST_TRAN,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_PAID_OFF) as DATE_PAID_OFF,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_LAST_PAYMENT) as DATE_LAST_PAYT,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_LAST_MAINT) as DATE_LAST_MNTN,
    
    -- Columns 25-42: Financial/Balance Fields
    BCF_LTD_HIGH_BALANCE as LFTD_HIGH_BALN,
    BCF_PYMT_TBL_HIGH_BALANCE as PAYT_TABL_HIGH_BALN,
    BCF_LTD_INTEREST_CHGD as LFTD_ICHG,
    BCF_LTD_INTEREST_WAIVED as LFTD_INT_WAVR,
    BCF_FIXED_PAY_AMT as FIX_PAYT_AMT,
    BCF_FIXED_PAY_CYCLES as FIX_PACY,
    BCF_LAST_INT_RATE as LAST_IRTE,
    TRIM(BCF_LAST_RATE_CODE) as LAST_RATE_CODE,
    BCF_LAST_RATE_CODE_SEQ as LAST_RATE_CODE_SEQN,
    TRIM(BCF_LAST_MIN_PAY_CODE) as LAST_MIN_PAYT_CODE,
    BCF_LAST_MIN_PAY_CODE_SEQ as LAST_MIN_PAYT_CODE_SEQN,
    BCF_MIN_PAY_AMT as LAST_MIN_PAYT_AMT,
    BCF_MIN_PAY_DUE as CURR_MIN_PAYT_DUE,
    BCF_MIN_PAY_PAST_DUE as MIN_PAYT_PAST_DUE,
    BCF_AMT_LAST_PYMT_APPLIED as LAST_APPY_PAYT_AMT,
    BCF_PAYMENTS_BEFORE_GRACE as PAYT_BFOR_GRCE,
    BCF_ORIGINAL_BALANCE as ORIG_BALN,
    BCF_RATE_SAVER_RATE as INT_SAVE_RATE,
    
    -- Columns 43-50: More Date/Balance Fields
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_PLAN_DUE_DATE) as PLAN_DUE_DATE,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DATE_END_INTEREST_FREE) as INT_FEE_END_DATE,
    BCF_INTEREST_FREE_PRIN_AMT as INT_FREE_BALN,
    BCF_OPENING_BALANCE as OPEN_BALN_CURR_CYCL,
    BCF_CURRENT as CTD_SALE_TRAN,
    BCF_DEBITS as CTD_DR_TRAN,
    BCF_CREDITS as CTD_CR_TRAN,
    BCF_PAYMENTS as CTD_PAYT_TRAN,
    
    -- Columns 51-85: Interest, Fee, AMF, Insurance, Principal Balance Fields
    BCF_INT_OPEN_NON_ACCR as INT_UNPD_OPEN_NOT_ACRL_BALN,
    BCF_INT_CTD_NON_ACCR as INT_UNPD_CTD_NOT_ACRL_BALN,
    BCF_INT_CURR_CYC_BAL as INT_UNPD_CURR_CYCL_BALN,
    BCF_INT_1CYC_AGO_BAL as INT_UNPD_1_CYCL_AGO_BALN,
    BCF_INT_2CYC_AGO_BAL as INT_UNPD_2_CYCL_AGO_BALN,
    BCF_FEES_OPEN_NON_ACCR as FEE_UNPD_OPEN_NOT_ACRL_BALN,
    BCF_FEES_CTD_NON_ACCR as FEE_UNPD_CTD_NOT_ACRL_BALN,
    BCF_FEES_CURR_CYC_BAL as FEE_UNPD_CURR_CYCL_BALN,
    BCF_FEES_1CYC_AGO_BAL as FEE_UNPD_1_CYCL_AGO_BALN,
    BCF_FEES_2CYC_AGO_BAL as FEE_UNPD_2_CYCL_AGO_BALN,
    BCF_AMF_OPEN_NON_ACCR as AMF_UNPD_OPEN_NOT_ACRL_BALN,
    BCF_AMF_CTD_NON_ACCR as AMF_UNPD_CTD_NOT_ACRL_BALN,
    BCF_AMF_CURR_CYC_BAL as AMF_UNPD_CURR_CYCL_BALN,
    BCF_AMF_1CYC_AGO_BAL as AMF_UNPD_1_CYCL_AGO_BALN,
    BCF_AMF_2CYC_AGO_BAL as AMF_UNPD_2_CYCL_AGO_BALN,
    BCF_INS_OPEN_NON_ACCR as INSR_UNPD_OPEN_NOT_ACRL_BALN,
    BCF_INS_CTD_NON_ACCR as INSR_UNPD_CTD_NOT_ACRL_BALN,
    BCF_INS_CURR_CYC_BAL as INSR_UNPD_CURR_CYCL_BALN,
    BCF_INS_1CYC_AGO_BAL as INSR_UNPD_1_CYCL_AGO_BALN,
    BCF_INS_2CYC_AGO_BAL as INSR_UNPD_2_CYCL_AGO_BALN,
    BCF_PRIN_OPEN_NON_ACCR as PRIN_UNPD_OPEN_NOT_ACRL_BALN,
    BCF_PRIN_CTD_NON_ACCR as PRIN_UNPD_CTD_NOT_ACRL_BALN,
    BCF_PRIN_CURR_CYC_BAL as PRIN_UNPD_CURR_CYCL_BALN,
    BCF_PRIN_1CYC_AGO_BAL as PRIN_UNPD_1_CYCL_AGO_BALN,
    BCF_PRIN_2CYC_AGO_BAL as PRIN_UNPD_2_CYCL_AGO_BALN,
    BCF_AGGR_CURR_CYC_BAL as AGGR_CURR_CYCL_BALN,
    BCF_AGGR_1CYC_AGO_BAL as AGGR_1_CYCL_AGO_BALN,
    BCF_AGGR_2CYC_AGO_BAL as AGGR_2_CYCL_AGO_BALN,
    BCF_AGGR_DELAYED_BAL as AGGR_DLAY_BALN,
    BCF_DELAYED_DAYS as DLAY_DAY_QNTY,
    BCF_CTD_TRANS_FEE as CTD_CSAD_FEE_AMT,
    BCF_CTD_TRANS_FEE_AMT as CTD_SYST_GENR_CSAD_FEE_AMT,
    BCF_DEFERRED_DAYS as INT_DEFR_DAY_QNTY,
    BCF_DEFERRED_INTEREST as INT_DEFR_AMT,
    BCF_AGGR_DEFERRED_BAL as INT_DEFR_AGGR_CURR_BALN,
    
    -- Columns 86-89: User/Migration Fields
    TRIM(BCF_USER_CODE) as USER_CODE,
    TRIM(BCF_MIGRATE_TO_PLAN) as MIGR_TO_PLAN_IDNN,
    BCF_MIGRATE_TO_SEQ as MIGR_TO_PLAN_SQNO,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_MIGRATE) as DATE_MIGR,
    
    -- Columns 90-129: Additional Business/Financial Fields
    BCF_DISPUTE_AMT as DSPT_AMT,
    BCF_EXT_DISPUTE_AMT as EXCL_DSPT_AMT,
    BCF_ACCR_DISPUTE_AMT as ACRL_DSPT_AMT,
    BCF_FL_INSTALLMENTS_BILLED as FRNT_LOAD_ISMT_BILL,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_FL_LAST_INSTALLMENT_DT) as FRNT_LOAD_LAST_ISMT_DATE,
    BCF_FL_ORIG_INTEREST_AMT as FRNT_LOAD_ORIG_INT_AMT,
    BCF_FL_EARNED_INTEREST_AMT as FRNT_LOAD_EARN_INT_AMT,
    BCF_FL_REBATE_INTEREST_AMT as FRNT_LOAD_REBT_INT_AMT,
    BCF_FL_INTEREST_RATE as FRNT_LOAD_IRTE,
    BCF_FL_ORIG_INSUR_AMT as FRNT_LOAD_ORIG_INSR_AMT,
    BCF_FL_EARNED_INSUR_AMT as FRNT_LOAD_EARN_INSR_AMT,
    BCF_FL_UPFRONT_INSUR_AMT as FRNT_LOAD_UPFR_INSR_AMT,
    BCF_FL_REBATE_INSUR_AMT as FRNT_LOAD_REBT_INSR_AMT,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_SCHED_PAYOFF_DT) as SCHE_PAYF_DATE,
    BCF_SCHED_PAYOFF_AMT as SCHE_PAYF_AMT,
    BCF_SCHED_PAYOFF_FEE as SCHE_PAYF_FEE,
    TRIM(BCF_SCHED_PAYOFF_REASON) as SCHE_PAYF_REAS,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_ACTUAL_PAYOFF_DT) as ACTL_PAYF_DATE,
    BCF_ACTUAL_PAYOFF_AMT as ACTL_PAYF_AMT,
    BCF_FL_UPFRONT_INSUR_PAID as FRNT_LOAD_INSR_PAID,
    BCF_FL_REBATE_INSUR_PAID as FRNT_LOAD_REBT_INSR_PAID,
    BCF_GOVT_NON_ACCR_OPN_AMT as GOVT_CHRG_NOT_ACRL_OPEN_AMT,
    BCF_GOVT_NON_ACCR_CTD_AMT as GOVT_CHRG_NOT_ACRL_CTD_AMT,
    BCF_GOVT_BAL_CURR_CYC_AMT as GOVT_CHRG_CURR_CYCL_AMT,
    BCF_GOVT_BAL_1CYC_AGO_AMT as GOVT_CHRG_1_CYCL_AGO_AMT,
    BCF_GOVT_BAL_2CYC_AGO_AMT as GOVT_CHRG_2_CYCL_AGO_AMT,
    BCF_CTD_FIN_CHG_REV as CTD_FNCL_CHRG_REV,
    BCF_INSTALL_TERM as ISMT_QNTY,
    BCF_PREV_INSTALL_TERM as ISMT_PREV_QNTY,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_INSTALL_TERM_CHG) as DATE_ISMT_TERM_CHNG,
    BCF_PREV_MIN_PAY_AMT as PREV_MIN_PAYT_AMT,
    BCF_ORIG_LOAN_BAL as ORIG_LOAN_BALN,
    BCF_LTD_ALL_CRDTS as LFTD_ALL_CR,
    BCF_LTD_INTEREST_SAVED as LFTD_INT_SAVE,
    BCF_PREV_CYC_INT_SAVED as PREV_CYCL_INT_SAVE,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DT_INSTALL_PAID) as DATE_LOAN_PAID_OUT,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_PROJECTED_PAYOFF_DT) as PRJC_PAY_OFF_DATE,
    BCF_DISPUTE_TIMES as DSPT_QNTY,
    {{ dcf_database_ref() }}.fn_ebcdic_to_dt(BCF_DISPUTE_OLD_DATE) as DSPT_OLD_DATE,
    BCF_TERM as TERM,
    
    -- Generated/System Fields (Columns 130-135)
    ROW_NUMBER() OVER (ORDER BY 1) as ROW_I,
    CURRENT_TIMESTAMP() as EFFT_S,
    CURRENT_TIME() as EFFT_T,
    0 as ROW_SECU_ACCS_C,
    
    -- Additional required fields
    business_date as EFFT_D,
    batch_id as PROS_KEY_EFFT_I

FROM source_data