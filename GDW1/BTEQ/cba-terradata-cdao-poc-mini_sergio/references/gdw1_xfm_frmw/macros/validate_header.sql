-- ============================================================================
-- Generic Header Validation Macro - IGSN Framework
-- ============================================================================
-- Purpose: Validates EBCDIC header records against current active business date
-- Replaces: DataStage file validation patterns (SQ20, etc.)
-- Pattern: DCF-integrated table-based validation with parameterized date fields
--
-- Business Logic:
--   1. Get current active business date from DCF_T_STRM_BUS_DT
--   2. Extract header records from header tracker table
--   3. Validate specified date field against active business date
--   4. Update processing status based on validation results
--   5. Fail macro if critical validation errors occur

{% macro validate_header(stream_name, header_date_field='BCF_DT_CURR_PROC') %}
  
  {% set log_prefix = "[VALIDATE_HEADER]" %}
  {{ log(log_prefix ~ " Starting header validation for stream: " ~ stream_name) }}
  
  {# Log header validation start to DCF_T_EXEC_LOG #}
  {% if execute %}
    {{ log_dcf_exec_msg('HEADER_VALIDATION', stream_name, 10, 'Header validation started for stream ' ~ stream_name) }}
  {% endif %}
  
  -- ========================================================================
  -- 1. GET CURRENT ACTIVE BUSINESS DATE FROM DCF
  -- ========================================================================
  
  {% set business_date_query %}
    SELECT 
      BUS_DT,
      STRM_NAME,
      STREAM_STATUS
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
    WHERE STRM_NAME = '{{ stream_name }}'
      AND PROCESSING_FLAG = 1  -- 1 = RUNNING
    ORDER BY BUS_DT DESC
    LIMIT 1
  {% endset %}
  
  {% if execute %}
    {% set business_date_result = run_query(business_date_query) %}
  {% else %}
    {% set business_date_result = none %}
  {% endif %}
  
  {% if execute %}
    {% if business_date_result.rows | length == 0 %}
      {{ log(log_prefix ~ " ERROR: No active business date found for stream: " ~ stream_name) }}
      {{ exceptions.raise_compiler_error("No active business date found for stream: " ~ stream_name) }}
    {% endif %}
    
    {% set current_business_date = business_date_result.rows[0][0] %}
    {% set stream_status = business_date_result.rows[0][2] %}
    
    {{ log(log_prefix ~ " Current active business date: " ~ current_business_date ~ " (Status: " ~ stream_status ~ ")") }}
  {% endif %}
  
  -- ========================================================================
  -- 2. DISCOVERY: GET HEADER RECORDS AWAITING VALIDATION
  -- ========================================================================
  
  {% set discovery_query %}
    SELECT 
      HEADER_TRACKER_ID,
      FEED_NM,
      SOURCE_FILE_NM,
      HEADER_FILE_NM,
      PROCESSING_STATUS,
      HEADER_METADATA,
      FILE_LOAD_TS,
      STREAM_NAME
    FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
    WHERE PROCESSING_STATUS = 'DISCOVERED'
    AND STREAM_NAME = '{{ stream_name }}'
    ORDER BY FILE_LOAD_TS ASC
  {% endset %}
  
  {% if execute %}
    {% set discovery_results = run_query(discovery_query) %}
    {% set files_to_validate = discovery_results.rows %}
    
    {% if files_to_validate | length == 0 %}
      {# Check if files are already validated for this stream #}
      {% set validated_check_query %}
        SELECT COUNT(*) as validated_count
        FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
        WHERE PROCESSING_STATUS = 'VALIDATED' AND STREAM_NAME = '{{ stream_name }}'
      {% endset %}
      {% set validated_results = run_query(validated_check_query) %}
      {% set validated_count = validated_results.rows[0][0] %}
      
      {% if validated_count > 0 %}
        {{ log(log_prefix ~ " Files already validated for stream: " ~ stream_name ~ " (count: " ~ validated_count ~ ")") }}
        {{ log_dcf_exec_msg('HEADER_VALIDATION', stream_name, 10, 'Header validation already completed - ' ~ validated_count ~ ' files previously validated for stream ' ~ stream_name) }}
      {% else %}
        {{ log(log_prefix ~ " ERROR: No files in DISCOVERED status found for stream: " ~ stream_name) }}
        {{ log_dcf_exec_msg('HEADER_VALIDATION', stream_name, 11, 'Header validation failed: No files in DISCOVERED status for stream ' ~ stream_name) }}
        {{ exceptions.raise_compiler_error("Header Validation Failed for stream " ~ stream_name ~ ": No files found in DISCOVERED status. Expected files to be loaded and ready for validation.") }}
      {% endif %}
    {% endif %}
    
    {{ log(log_prefix ~ " Found " ~ (files_to_validate | length) ~ " files to validate for stream: " ~ stream_name) }}
  {% endif %}
  
  -- ========================================================================
  -- 3. VALIDATION LOGIC: PROCESS EACH HEADER FILE
  -- ========================================================================
  
  {% set validation_results = [] %}
  {% set critical_failures = [] %}
  
  {% if execute %}
    {% for file_record in files_to_validate %}
      {% set header_tracker_id = file_record[0] %}
      {% set feed_name = file_record[1] %}
      {% set source_file_name = file_record[2] %}
      {% set header_file_name = file_record[3] %}
      {% set header_metadata = file_record[5] %}
      
      {{ log(log_prefix ~ " Validating file: " ~ header_file_name) }}
      
      -- ====================================================================
      -- 3.1 EXTRACT DATE FROM HEADER METADATA
      -- ====================================================================
      
      {% set date_extraction_query %}
        SELECT 
          '{{ header_tracker_id }}' AS header_tracker_id,
          '{{ source_file_name }}' AS source_file_name,
          '{{ header_file_name }}' AS header_file_name,
          
          -- Extract specified date field from JSON metadata
          HEADER_METADATA['file_metadata']['header_records'][0]['{{ header_date_field }}']::STRING AS extracted_date,
          
          -- Extract total header records count
          HEADER_METADATA['file_metadata']['total_header_records']::NUMBER AS total_header_records,
          
          CURRENT_TIMESTAMP() AS validation_timestamp
        FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
        WHERE HEADER_TRACKER_ID = {{ header_tracker_id }}
      {% endset %}
      
      {% set extraction_result = run_query(date_extraction_query) %}
      {% set file_data = extraction_result.rows[0] %}
      
      {% set extracted_date = file_data[3] %}
      {% set total_headers = file_data[4] %}
      
      -- ====================================================================
      -- 3.2 COMPREHENSIVE VALIDATION CHECKS
      -- ====================================================================
      
      {% set validation_status = 'VALIDATED' %}
      {% set validation_message = 'Header validation successful' %}
      {% set is_critical_failure = false %}
      
      -- Check 1: Date field presence
      {% if not extracted_date or extracted_date == '' %}
        {% set validation_status = 'ERROR' %}
        {% set validation_message = header_date_field ~ ' field is missing or empty in header metadata' %}
        {% set is_critical_failure = true %}
      
      -- Check 2: Date format validation (should be 8-digit YYYYMMDD)
      {% elif not (extracted_date | string | length == 8 and extracted_date.isdigit()) %}
        {% set validation_status = 'ERROR' %}
        {% set validation_message = 'Invalid date format in ' ~ header_date_field ~ ': ' ~ extracted_date ~ ', expected YYYYMMDD format' %}
        {% set is_critical_failure = true %}
      
      -- Check 3: Date validity and business date comparison
      {% else %}
        {% set date_validation_query %}
          SELECT 
            CASE 
              WHEN '{{ extracted_date }}' = TO_CHAR(TO_DATE('{{ current_business_date }}'), 'YYYYMMDD') THEN 'MATCH'
              WHEN TRY_TO_DATE('{{ extracted_date }}', 'YYYYMMDD') IS NULL THEN 'INVALID_DATE'
              WHEN TRY_TO_DATE('{{ extracted_date }}', 'YYYYMMDD') > CURRENT_DATE() THEN 'FUTURE_DATE'
              ELSE 'DATE_MISMATCH'
            END AS date_validation_result,
            '{{ extracted_date }}' AS file_date,
            TO_CHAR(TO_DATE('{{ current_business_date }}'), 'YYYYMMDD') AS expected_date
        {% endset %}
        
        {% set date_check_result = run_query(date_validation_query) %}
        {% set date_validation_result = date_check_result.rows[0][0] %}
        {% set expected_date_formatted = date_check_result.rows[0][2] %}
        
        {% if date_validation_result == 'MATCH' %}
          {% set validation_status = 'VALIDATED' %}
          {% set validation_message = 'Processing date validation successful: ' ~ extracted_date ~ ' matches active business date' %}
        
        {% elif date_validation_result == 'INVALID_DATE' %}
          {% set validation_status = 'ERROR' %}
          {% set validation_message = 'Invalid date value in ' ~ header_date_field ~ ': ' ~ extracted_date %}
          {% set is_critical_failure = true %}
        
        {% elif date_validation_result == 'FUTURE_DATE' %}
          {% set validation_status = 'REJECTED' %}
          {% set validation_message = 'Future processing date not allowed: ' ~ extracted_date %}
          {% set is_critical_failure = true %}
        
        {% else %}  -- DATE_MISMATCH
          {% set validation_status = 'DATE_MISMATCH' %}
          {% set validation_message = 'Processing date mismatch - file: ' ~ extracted_date ~ ', active business date: ' ~ expected_date_formatted %}
          {% set is_critical_failure = true %}
        
        {% endif %}
      {% endif %}
      
      -- ====================================================================
      -- 3.3 UPDATE HEADER TRACKER STATUS
      -- ====================================================================
      
      {% set update_status_query %}
        UPDATE {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
        SET 
          PROCESSING_STATUS = '{{ validation_status }}',
          PROCESSING_MSG = '{{ validation_message }}',
          PROCESSING_TS = CURRENT_TIMESTAMP(),
          EXTRACTED_PROCESSING_DT = '{{ extracted_date }}',
          EXPECTED_PROCESSING_DT = TO_CHAR(TO_DATE('{{ current_business_date }}'), 'YYYYMMDD'),
          UPDATED_BY = CURRENT_USER(),
          UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
        WHERE HEADER_TRACKER_ID = {{ header_tracker_id }}
      {% endset %}
      
      {% do run_query(update_status_query) %}
      
      -- Track results
      {% do validation_results.append({
        'file_name': header_file_name,
        'status': validation_status,
        'message': validation_message,
        'extracted_date': extracted_date,
        'is_critical': is_critical_failure
      }) %}
      
      {% if is_critical_failure %}
        {% do critical_failures.append(validation_message) %}
      {% endif %}
      
      {{ log(log_prefix ~ " File " ~ header_file_name ~ " validation result: " ~ validation_status ~ " - " ~ validation_message) }}
      
    {% endfor %}
  {% endif %}
  
  -- ========================================================================
  -- 4. VALIDATION SUMMARY AND FAILURE HANDLING
  -- ========================================================================
  
  {% if execute %}
    {% set total_files = validation_results | length %}
    {% set successful_files = validation_results | selectattr('status', 'equalto', 'VALIDATED') | list | length %}
    {% set failed_files = total_files - successful_files %}
    
    {{ log(log_prefix ~ " Validation Summary for stream " ~ stream_name ~ ":") }}
    {{ log(log_prefix ~ "   Total files processed: " ~ total_files) }}
    {{ log(log_prefix ~ "   Successfully validated: " ~ successful_files) }}
    {{ log(log_prefix ~ "   Failed validation: " ~ failed_files) }}
    
    -- Check for critical failures
    {% if critical_failures | length > 0 %}
      {{ log(log_prefix ~ " CRITICAL VALIDATION FAILURES DETECTED:") }}
      {% for failure in critical_failures %}
        {{ log(log_prefix ~ "   ERROR: " ~ failure) }}
      {% endfor %}
      
      {# Log critical failures to DCF_T_EXEC_LOG #}
      {{ log_dcf_exec_msg('HEADER_VALIDATION', stream_name, 11, 'Header validation failed with ' ~ (critical_failures | length) ~ ' critical errors: ' ~ (critical_failures | join('; '))) }}
      
      {{ log(log_prefix ~ " Header validation FAILED for stream " ~ stream_name ~ " due to critical errors") }}
      {{ exceptions.raise_compiler_error("Header Validation Failed for stream " ~ stream_name ~ ": " ~ (critical_failures | join('; '))) }}
    {% endif %}
    
    {% if successful_files == 0 and total_files > 0 %}
      {{ log(log_prefix ~ " All files failed validation - stopping processing") }}
      {# Log all files failed to DCF_T_EXEC_LOG #}
      {{ log_dcf_exec_msg('HEADER_VALIDATION', stream_name, 11, 'Header validation failed: All ' ~ total_files ~ ' files failed validation for stream ' ~ stream_name) }}
      {{ exceptions.raise_compiler_error("Header Validation Failed for stream " ~ stream_name ~ ": No files passed validation") }}
    {% endif %}
    
    {# Handle success case: either files were processed or already validated #}
    {% if total_files > 0 %}
      {# Files were processed this time #}
      {{ log_dcf_exec_msg('HEADER_VALIDATION', stream_name, 10, 'Header validation completed successfully for stream ' ~ stream_name ~ ': ' ~ successful_files ~ ' of ' ~ total_files ~ ' files validated') }}
      {{ log(log_prefix ~ " Header validation completed successfully for stream " ~ stream_name) }}
    {% else %}
      {# No files to process - already handled in idempotent check above #}
      {{ log(log_prefix ~ " Header validation completed (idempotent) for stream " ~ stream_name) }}
    {% endif %}
  {% endif %}
  
  SELECT 'VALIDATION_PASSED' as status

{% endmacro %}


-- ============================================================================
-- HELPER MACRO: Get current active business date from DCF
-- ============================================================================
{% macro get_active_business_date(stream_name) %}
  
  {% set business_date_query %}
    SELECT 
      BUS_DT,
      STREAM_STATUS
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
    WHERE STRM_NAME = '{{ stream_name }}'
      AND PROCESSING_FLAG = 1  -- 1 = RUNNING
    ORDER BY BUS_DT DESC
    LIMIT 1
  {% endset %}
  
  {% if execute %}
    {% set result = run_query(business_date_query) %}
    {% if result.rows | length > 0 %}
      {{ return(result.rows[0][0]) }}
    {% else %}
      {{ return(none) }}
    {% endif %}
  {% endif %}
  
  {{ return(none) }}

{% endmacro %}


-- ============================================================================
-- HELPER MACRO: Check header validation readiness
-- ============================================================================
{% macro check_header_validation_readiness(stream_name) %}
  
  {% set readiness_query %}
    SELECT 
      COUNT(*) AS files_discovered,
      COUNT(CASE WHEN PROCESSING_STATUS = 'DISCOVERED' THEN 1 END) AS files_awaiting_validation,
      MAX(FILE_LOAD_TS) AS latest_file_time,
      LISTAGG(HEADER_FILE_NM, ', ') AS file_list
    FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
    WHERE STREAM_NAME = '{{ stream_name }}'
      AND FEED_NM = '{{ stream_name.split('_')[0] }}'
      AND FILE_LOAD_TS >= CURRENT_DATE() - INTERVAL '1 day'
  {% endset %}
  
  {% if execute %}
    {% set result = run_query(readiness_query) %}
    {% set files_discovered = result.rows[0][0] %}
    {% set files_awaiting = result.rows[0][1] %}
    {% set latest_time = result.rows[0][2] %}
    {% set file_list = result.rows[0][3] %}
    
    {{ log("[HEADER_READINESS] Files discovered in last 24h: " ~ files_discovered) }}
    {{ log("[HEADER_READINESS] Files awaiting validation: " ~ files_awaiting) }}
    {{ log("[HEADER_READINESS] Latest file time: " ~ latest_time) }}
    {{ log("[HEADER_READINESS] Files: " ~ file_list) }}
    
    {{ return({
      'files_discovered': files_discovered,
      'files_awaiting': files_awaiting,
      'latest_file_time': latest_time,
      'file_list': file_list
    }) }}
  {% endif %}
  
  {{ return({'files_discovered': 0, 'files_awaiting': 0}) }}

{% endmacro %}


-- ============================================================================
-- TESTING MACRO: Validate specific header file for testing
-- ============================================================================
{% macro test_validate_single_header(header_tracker_id, expected_business_date, header_date_field='BCF_DT_CURR_PROC') %}
  
  {% set log_prefix = "[TEST_HEADER_VALIDATION]" %}
  {{ log(log_prefix ~ " Testing header validation for tracker ID: " ~ header_tracker_id) }}
  
  {% set test_query %}
    SELECT 
      HEADER_TRACKER_ID,
      FEED_NM,
      HEADER_FILE_NM,
      PROCESSING_STATUS,
      HEADER_METADATA['file_metadata']['header_records'][0]['{{ header_date_field }}']::STRING AS extracted_date,
      '{{ expected_business_date }}' AS expected_date,
      CASE 
        WHEN HEADER_METADATA['file_metadata']['header_records'][0]['{{ header_date_field }}']::STRING = '{{ expected_business_date }}'
        THEN 'MATCH'
        ELSE 'MISMATCH'
      END AS date_comparison
    FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
    WHERE HEADER_TRACKER_ID = {{ header_tracker_id }}
  {% endset %}
  
  {% if execute %}
    {% set result = run_query(test_query) %}
    {% if result.rows | length > 0 %}
      {% set row = result.rows[0] %}
      {{ log(log_prefix ~ " Header file: " ~ row[2]) }}
      {{ log(log_prefix ~ " Current status: " ~ row[3]) }}
      {{ log(log_prefix ~ " Extracted date: " ~ row[4]) }}
      {{ log(log_prefix ~ " Expected date: " ~ row[5]) }}
      {{ log(log_prefix ~ " Date comparison: " ~ row[6]) }}
      
      {{ return(row[6]) }}
    {% else %}
      {{ log(log_prefix ~ " ERROR: Header tracker ID not found: " ~ header_tracker_id) }}
      {{ return('NOT_FOUND') }}
    {% endif %}
  {% endif %}
  
  {{ return('ERROR') }}

{% endmacro %}


-- ============================================================================
-- OPERATION MACRO: Run header validation directly
-- ============================================================================
{% macro header_validation_op(stream_name) %}
  {#- 
    Run header validation directly as an operation
    Usage: dbt run-operation header_validation_op --args '{stream_name: "BCFINSG"}'
  -#}
  
  {% if not stream_name %}
    {{ exceptions.raise_compiler_error("stream_name parameter is required. Usage: dbt run-operation header_validation_op --args '{stream_name: \"BCFINSG\"}'") }}
  {% endif %}
  
  {{ print("📋 Running Header Validation for stream: " ~ stream_name) }}
  
  {% if execute %}
    {# Call the header validation macro directly #}
    {% set result = validate_header(stream_name) %}
    
    {{ print("✅ Header validation completed with result: " ~ result) }}
  {% endif %}
  
{% endmacro %}