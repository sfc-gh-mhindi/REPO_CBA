select
acct_i, srce_syst_acct_numb, corp_idnn, plan_idnn , plan_sqno , stor_idnn, plan_type, plan_catg , post_pur_stus , term_stus , int_defr_stus,
payt_defr_stus , time_bill , spec_term_cycl , int_defr_cycl , payt_defr_cycl
, date_spec_term_end , date_int_defr_end , date_payt_defr_end , date_frst_tran , date_paid_off , date_last_payt , date_last_mntn , lftd_high_baln , payt_tabl_high_baln
, lftd_ichg , lftd_int_wavr , fix_payt_amt , fix_pacy , last_irte , last_rate_code , last_rate_code_seqn, last_min_payt_code , last_min_payt_code_seqn , last_min_payt_amt
, curr_min_payt_due , min_payt_past_due , last_appy_payt_amt , payt_bfor_grce , orig_baln , int_save_rate , plan_due_date , int_fee_end_date , int_free_baln , open_baln_curr_cycl
, ctd_sale_tran , ctd_dr_tran , ctd_cr_tran , ctd_payt_tran , int_unpd_open_not_acrl_baln , int_unpd_ctd_not_acrl_baln , int_unpd_curr_cycl_baln , int_unpd_1_cycl_ago_baln , int_unpd_2_cycl_ago_baln , fee_unpd_open_not_acrl_baln
, fee_unpd_ctd_not_acrl_baln , fee_unpd_curr_cycl_baln , fee_unpd_1_cycl_ago_baln , fee_unpd_2_cycl_ago_baln , amf_unpd_open_not_acrl_baln , amf_unpd_ctd_not_acrl_baln , amf_unpd_curr_cycl_baln , amf_unpd_1_cycl_ago_baln , amf_unpd_2_cycl_ago_baln
, insr_unpd_open_not_acrl_baln , insr_unpd_ctd_not_acrl_baln , insr_unpd_curr_cycl_baln , insr_unpd_1_cycl_ago_baln , insr_unpd_2_cycl_ago_baln , prin_unpd_open_not_acrl_baln , prin_unpd_ctd_not_acrl_baln , prin_unpd_curr_cycl_baln , prin_unpd_1_cycl_ago_baln
, prin_unpd_2_cycl_ago_baln , aggr_curr_cycl_baln , aggr_1_cycl_ago_baln , aggr_2_cycl_ago_baln , aggr_dlay_baln , dlay_day_qnty , ctd_csad_fee_amt , ctd_syst_genr_csad_fee_amt , int_defr_day_qnty , int_defr_amt , int_defr_aggr_curr_baln
, user_code , migr_to_plan_idnn , migr_to_plan_sqno, date_migr
, dspt_amt , excl_dspt_amt , acrl_dspt_amt , frnt_load_ismt_bill , frnt_load_last_ismt_date , frnt_load_orig_int_amt , frnt_load_earn_int_amt , frnt_load_rebt_int_amt , frnt_load_irte , frnt_load_orig_insr_amt
, frnt_load_earn_insr_amt , frnt_load_upfr_insr_amt , frnt_load_rebt_insr_amt, sche_payf_date
, sche_payf_amt , sche_payf_fee , sche_payf_reas, actl_payf_date
, actl_payf_amt , frnt_load_insr_paid , frnt_load_rebt_insr_paid , govt_chrg_not_acrl_open_amt
, govt_chrg_not_acrl_ctd_amt , govt_chrg_curr_cycl_amt , govt_chrg_1_cycl_ago_amt , govt_chrg_2_cycl_ago_amt , ctd_fncl_chrg_rev , ismt_qnty , ismt_prev_qnty , date_ismt_term_chng , prev_min_payt_amt , orig_loan_baln , lftd_all_cr , lftd_int_save , prev_cycl_int_save
, date_loan_paid_out , prjc_pay_off_date , dspt_qnty , dspt_old_date , term
from
npd_d12_dmn_gdwmig_ibrg.pdsrccs.plan_baln_segm_mstr_npw
where true
minus
select
acct_i, trim(srce_syst_acct_numb), trim(corp_idnn), trim(plan_idnn) , plan_sqno, trim(stor_idnn), trim(plan_type), trim(plan_catg), trim(post_pur_stus) , trim(term_stus) , trim(int_defr_stus),
trim(payt_defr_stus) , time_bill , spec_term_cycl , int_defr_cycl , payt_defr_cycl
, date_spec_term_end , date_int_defr_end , date_payt_defr_end , date_frst_tran , date_paid_off , date_last_payt , date_last_mntn , lftd_high_baln , payt_tabl_high_baln
, lftd_ichg , lftd_int_wavr , fix_payt_amt , fix_pacy , last_irte , trim(last_rate_code), last_rate_code_seqn, trim(last_min_payt_code), last_min_payt_code_seqn , last_min_payt_amt
, curr_min_payt_due , min_payt_past_due , last_appy_payt_amt , payt_bfor_grce , orig_baln , int_save_rate , plan_due_date , int_fee_end_date , int_free_baln , open_baln_curr_cycl
, ctd_sale_tran , ctd_dr_tran , ctd_cr_tran , ctd_payt_tran , int_unpd_open_not_acrl_baln , int_unpd_ctd_not_acrl_baln , int_unpd_curr_cycl_baln , int_unpd_1_cycl_ago_baln , int_unpd_2_cycl_ago_baln , fee_unpd_open_not_acrl_baln
, fee_unpd_ctd_not_acrl_baln , fee_unpd_curr_cycl_baln , fee_unpd_1_cycl_ago_baln , fee_unpd_2_cycl_ago_baln , amf_unpd_open_not_acrl_baln , amf_unpd_ctd_not_acrl_baln , amf_unpd_curr_cycl_baln , amf_unpd_1_cycl_ago_baln , amf_unpd_2_cycl_ago_baln
, insr_unpd_open_not_acrl_baln , insr_unpd_ctd_not_acrl_baln , insr_unpd_curr_cycl_baln , insr_unpd_1_cycl_ago_baln , insr_unpd_2_cycl_ago_baln , prin_unpd_open_not_acrl_baln , prin_unpd_ctd_not_acrl_baln , prin_unpd_curr_cycl_baln , prin_unpd_1_cycl_ago_baln
, prin_unpd_2_cycl_ago_baln , aggr_curr_cycl_baln , aggr_1_cycl_ago_baln , aggr_2_cycl_ago_baln , aggr_dlay_baln , dlay_day_qnty , ctd_csad_fee_amt , ctd_syst_genr_csad_fee_amt , int_defr_day_qnty , int_defr_amt , int_defr_aggr_curr_baln
, trim(user_code) user_code , trim(migr_to_plan_idnn) migr_to_plan_idnn , migr_to_plan_sqno, date_migr
, dspt_amt , excl_dspt_amt , acrl_dspt_amt , frnt_load_ismt_bill , frnt_load_last_ismt_date , frnt_load_orig_int_amt , frnt_load_earn_int_amt , frnt_load_rebt_int_amt , frnt_load_irte , frnt_load_orig_insr_amt
, frnt_load_earn_insr_amt , frnt_load_upfr_insr_amt , frnt_load_rebt_insr_amt, sche_payf_date
, sche_payf_amt , sche_payf_fee , trim(sche_payf_reas) sche_payf_reas, actl_payf_date
, actl_payf_amt , frnt_load_insr_paid , frnt_load_rebt_insr_paid , govt_chrg_not_acrl_open_amt
, govt_chrg_not_acrl_ctd_amt , govt_chrg_curr_cycl_amt , govt_chrg_1_cycl_ago_amt , govt_chrg_2_cycl_ago_amt , ctd_fncl_chrg_rev , ismt_qnty , ismt_prev_qnty , date_ismt_term_chng , prev_min_payt_amt , orig_loan_baln , lftd_all_cr , lftd_int_save , prev_cycl_int_save
, date_loan_paid_out , prjc_pay_off_date , dspt_qnty , dspt_old_date , term
from
npd_d12_dmn_gdwmig_ibrg_v.pvdata.plan_baln_segm_mstr
where true
;