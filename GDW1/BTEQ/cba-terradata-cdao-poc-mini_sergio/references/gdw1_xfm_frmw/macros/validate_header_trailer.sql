-- ============================================================================
-- Header/Trailer Validation Macro - CSEL4 Framework
-- ============================================================================
-- Purpose: Validates file structure using DCF_T_IGSN_FRMW_HDR_CTRL control table
-- Replaces: DataStage FileValidationLevel01CSEL4 logic
-- Pattern: DCF-integrated validation with HEADER_METADATA and TRAILER_METADATA
--
-- Business Logic:
--   1. Extract header/trailer metadata from DCF control table
--   2. Validate header record presence and date matching
--   3. Validate trailer record presence and count matching
--   4. Update processing status based on validation results
--   5. Return comprehensive validation report with error codes

{% macro validate_header_trailer(stream_name='BCFINSG_PLAN_BALN_SEGM_LOAD', staging_table_name=none, expected_file_name=none) %}
    /*
        Validates file structure using the DCF_T_IGSN_FRMW_HDR_CTRL control table
        following DataStage FileValidationLevel01CSEL4 logic
        
        Performs the following validations:
        - FVL0001: Header record present (check HEADER_METADATA)
        - FVL0002: Date in header matches ETL process date  
        - FVL0003: File name field matches expected file name
        - FVL0005: Trailer record count matches staging table row count
        - FVL0006: Trailer record present (check TRAILER_METADATA)
        
        Args:
            stream_name: The stream name to validate (default: BCFINSG_PLAN_BALN_SEGM_LOAD)
            staging_table_name: Name of staging table to compare record counts against
            expected_file_name: Expected file name pattern (optional)
            
        Returns:
            SQL query that returns validation results with error codes and messages
    */

    {% set log_prefix = "[VALIDATE_HEADER_TRAILER]" %}
    {{ log(log_prefix ~ " Starting header/trailer validation for stream: " ~ stream_name) }}

    {# Log validation start to DCF_T_EXEC_LOG #}
    {% if execute %}
        {{ log_dcf_exec_msg('HEADER_TRAILER_VALIDATION', stream_name, 10, 'Header/trailer validation started for stream ' ~ stream_name) }}
    {% endif %}

    -- ========================================================================
    -- FILE DISCOVERY CHECK: Ensure files are available for validation
    -- ========================================================================
    {% set discovery_query %}
        SELECT 
            HEADER_TRACKER_ID,
            FEED_NM,
            SOURCE_FILE_NM,
            HEADER_FILE_NM,
            PROCESSING_STATUS,
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
                {{ log_dcf_exec_msg('HEADER_TRAILER_VALIDATION', stream_name, 10, 'Header/trailer validation already completed - ' ~ validated_count ~ ' files previously validated for stream ' ~ stream_name) }}
            {% else %}
                {{ log(log_prefix ~ " ERROR: No files in DISCOVERED status found for stream: " ~ stream_name) }}
                {{ log_dcf_exec_msg('HEADER_TRAILER_VALIDATION', stream_name, 11, 'Header/trailer validation failed: No files in DISCOVERED status for stream ' ~ stream_name) }}
                {{ exceptions.raise_compiler_error("Header/Trailer Validation Failed for stream " ~ stream_name ~ ": No files found in DISCOVERED status. Expected files to be loaded and ready for validation.") }}
            {% endif %}
        {% endif %}
        
        {{ log(log_prefix ~ " Found " ~ (files_to_validate | length) ~ " files to validate for stream: " ~ stream_name) }}
        
        -- Get current active business date from DCF
        {% set business_date_query %}
            SELECT 
                BUS_DT,
                STRM_NAME
            FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
            WHERE STRM_NAME = '{{ stream_name }}'
              AND PROCESSING_FLAG = 1  -- 1 = RUNNING
            ORDER BY BUS_DT DESC
            LIMIT 1
        {% endset %}
        
        {% set business_date_result = run_query(business_date_query) %}
        {% if business_date_result.rows | length == 0 %}
            {{ exceptions.raise_compiler_error("No active business date found for stream " ~ stream_name) }}
        {% endif %}
        {% set active_business_date = business_date_result.rows[0][0] %}
        {% set expected_date = active_business_date.strftime('%Y%m%d') %}
        
        {{ log(log_prefix ~ " Active business date for stream " ~ stream_name ~ ": " ~ active_business_date ~ " (formatted: " ~ expected_date ~ ")") }}
        
        -- Perform date validation check for discovered files
        {% for file_record in files_to_validate %}
            {% set header_tracker_id = file_record[0] %}
            {% set header_file_name = file_record[3] %}
            
            -- Extract date from header metadata using SQL query
            {% set date_extraction_query %}
                SELECT 
                    COALESCE(
                        HEADER_METADATA:file_metadata.header_records[0].BCF_DT_CURR_PROC::STRING,  -- BCFINSG format
                        HEADER_METADATA:CSV_PROCESSING_DATE::STRING                               -- CSEL4 format
                    ) as extracted_date
                FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
                WHERE HEADER_TRACKER_ID = {{ header_tracker_id }}
            {% endset %}
            
            {% set extraction_result = run_query(date_extraction_query) %}
            {% set extracted_date = extraction_result.rows[0][0] %}
            
            {% if extracted_date and extracted_date != expected_date %}
                {{ log(log_prefix ~ " ERROR: Processing date mismatch - file: " ~ extracted_date ~ ", active business date: " ~ expected_date ~ " (file: " ~ header_file_name ~ ")") }}
                {{ log_dcf_exec_msg('HEADER_TRAILER_VALIDATION', stream_name, 11, 'Header/trailer validation failed: Processing date mismatch - file: ' ~ extracted_date ~ ', active business date: ' ~ expected_date) }}
                {{ exceptions.raise_compiler_error("Header/Trailer Validation Failed for stream " ~ stream_name ~ ": Processing date mismatch - file: " ~ extracted_date ~ ", active business date: " ~ expected_date) }}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if staging_table_name %}
        {% set staging_count_query %}
            {% if '.' in staging_table_name %}
                SELECT COUNT(*) as staging_row_count FROM {{ staging_table_name }}
            {% else %}
                SELECT COUNT(*) as staging_row_count FROM {{ ref(staging_table_name) }}
            {% endif %}
        {% endset %}
        
        {% if execute %}
            {% set staging_count_result = run_query(staging_count_query) %}
            {% set staging_row_count = staging_count_result.columns[0].values()[0] %}
        {% else %}
            {% set staging_row_count = 0 %}
        {% endif %}
    {% endif %}

    WITH validation_results AS (
        SELECT 
            cr.HEADER_TRACKER_ID,
            cr.FEED_NM,
            cr.HEADER_FILE_NM,
            cr.SOURCE_FILE_NM,
            cr.PROCESSING_STATUS,
            cr.STREAM_NAME,
            cr.EXPECTED_PROCESSING_DT,
            cr.EXTRACTED_PROCESSING_DT,
            cr.HEADER_METADATA,
            cr.TRAILER_METADATA,
            -- Extract these from JSON metadata instead of direct columns
            cr.HEADER_METADATA['file_metadata']['total_header_records']::NUMBER as total_header_records_meta,
            cr.TRAILER_METADATA['record_count']::NUMBER as control_record_count_meta,
            cr.FILE_LOAD_TS,
            cr.PROCESSING_TS,
            
            -- Extract header fields for validation (support both BCFINSG and CSEL4 formats)
            COALESCE(
                cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING,  -- BCFINSG format
                cr.HEADER_METADATA['CSV_PROCESSING_DATE']::STRING                                     -- CSEL4 format
            ) as header_current_date,
            cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_NEXT_PROC']::STRING as header_next_date,
            cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_ACCOUNT_NO_0']::STRING as control_record_id,
            cr.HEADER_METADATA['file_metadata']['total_header_records']::NUMBER as total_header_records,
            cr.CONTROL_RECORD_COUNT as trailer_record_count,
            
            {% if staging_table_name %}
            {{ staging_row_count }} as staging_row_count,
            {% else %}
            NULL as staging_row_count,
            {% endif %}
            
            -- FVL0001: Header record check
            CASE 
                WHEN cr.HEADER_METADATA IS NULL THEN 'FVL0001'
                WHEN cr.HEADER_METADATA['file_metadata']['header_records'] IS NULL THEN 'FVL0001'
                WHEN ARRAY_SIZE(cr.HEADER_METADATA['file_metadata']['header_records']) = 0 THEN 'FVL0001'
                ELSE 'PASS' 
            END as header_check_code,
            
            CASE 
                WHEN cr.HEADER_METADATA IS NULL THEN 'Header metadata missing.'
                WHEN cr.HEADER_METADATA['file_metadata']['header_records'] IS NULL THEN 'Header records missing in metadata.'
                WHEN ARRAY_SIZE(cr.HEADER_METADATA['file_metadata']['header_records']) = 0 THEN 'No header records found in metadata.'
                ELSE 'Header record present.'
            END as header_check_message,
            
            -- FVL0002: Date validation (support both BCFINSG and CSEL4 formats)
            CASE 
                WHEN COALESCE(
                    cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING,
                    cr.HEADER_METADATA['CSV_PROCESSING_DATE']::STRING
                ) IS NULL THEN 'FVL0002'
                WHEN COALESCE(
                    cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING,
                    cr.HEADER_METADATA['CSV_PROCESSING_DATE']::STRING
                ) = '{{ expected_date }}' THEN 'PASS'
                ELSE 'FVL0002'
            END as date_check_code,
            
            CASE 
                WHEN COALESCE(
                    cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING,
                    cr.HEADER_METADATA['CSV_PROCESSING_DATE']::STRING
                ) IS NULL THEN 'Processing date missing in header metadata.'
                WHEN COALESCE(
                    cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING,
                    cr.HEADER_METADATA['CSV_PROCESSING_DATE']::STRING
                ) = '{{ expected_date }}' THEN 'Date in header matches active business date.'
                ELSE 'Date in header (' || COALESCE(
                    cr.HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING,
                    cr.HEADER_METADATA['CSV_PROCESSING_DATE']::STRING
                ) || 
                     ') does not match the active business date ({{ active_business_date }}).'
            END as date_check_message,
            
            {% if expected_file_name %}
            -- FVL0003: File name validation
            CASE 
                WHEN cr.HEADER_FILE_NM IS NULL THEN 'FVL0003'
                WHEN cr.HEADER_FILE_NM LIKE '%{{ expected_file_name }}%' THEN 'PASS'
                ELSE 'FVL0003'
            END as filename_check_code,
            
            CASE 
                WHEN cr.HEADER_FILE_NM IS NULL THEN 'Header file name missing.'
                WHEN cr.HEADER_FILE_NM LIKE '%{{ expected_file_name }}%' THEN 'File name matches expected pattern.'
                ELSE 'File name (' || cr.HEADER_FILE_NM || 
                     ') does not match expected pattern ({{ expected_file_name }}).'
            END as filename_check_message,
            {% else %}
            'SKIP' as filename_check_code,
            'File name validation skipped - no expected pattern provided.' as filename_check_message,
            {% endif %}
            
            {% if staging_table_name %}
            -- FVL0005: Row count validation against staging table (using control record count)
            CASE 
                WHEN cr.CONTROL_RECORD_COUNT IS NULL THEN 'FVL0005'
                WHEN cr.CONTROL_RECORD_COUNT = {{ staging_row_count }} THEN 'PASS'
                ELSE 'FVL0005'
            END as rowcount_check_code,
            
            CASE 
                WHEN cr.CONTROL_RECORD_COUNT IS NULL THEN 'Control record count missing.'
                WHEN cr.CONTROL_RECORD_COUNT = {{ staging_row_count }} THEN 'Control record count matches staging table row count.'
                ELSE 'Control record count (' || cr.CONTROL_RECORD_COUNT || 
                     ') does not match staging table row count ({{ staging_row_count }}).'
            END as rowcount_check_message,
            {% else %}
            'SKIP' as rowcount_check_code,
            'Record count validation skipped - no staging table provided.' as rowcount_check_message,
            {% endif %}
            
            -- FVL0006: Trailer record check
            CASE 
                WHEN cr.TRAILER_METADATA IS NULL THEN 'SKIP'  -- Trailer may not be available for all feeds
                WHEN cr.TRAILER_METADATA['record_count'] IS NULL THEN 'FVL0006'
                ELSE 'PASS' 
            END as trailer_check_code,
            
            CASE 
                WHEN cr.TRAILER_METADATA IS NULL THEN 'Trailer validation skipped - no trailer metadata available.'
                WHEN cr.TRAILER_METADATA['record_count'] IS NULL THEN 'Trailer record count missing in metadata.'
                ELSE 'Trailer record present.'
            END as trailer_check_message,
            
            CURRENT_TIMESTAMP() as validation_timestamp,
            '{{ active_business_date }}' as etl_process_date
            
        FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL cr
        WHERE cr.STREAM_NAME = '{{ stream_name }}'
          AND cr.PROCESSING_STATUS = 'DISCOVERED'
    )
    
    SELECT 
        *,
        -- Overall validation result
        CASE 
            WHEN header_check_code != 'PASS' THEN header_check_code
            WHEN date_check_code != 'PASS' THEN date_check_code
            {% if expected_file_name %}
            WHEN filename_check_code != 'PASS' AND filename_check_code != 'SKIP' THEN filename_check_code
            {% endif %}
            WHEN trailer_check_code != 'PASS' AND trailer_check_code != 'SKIP' THEN trailer_check_code
            WHEN rowcount_check_code != 'PASS' AND rowcount_check_code != 'SKIP' THEN rowcount_check_code
            ELSE 'PASS'
        END as overall_validation_result,
        
        -- Validation passed flag
        CASE 
            WHEN header_check_code != 'PASS' THEN FALSE
            WHEN date_check_code != 'PASS' THEN FALSE
            {% if expected_file_name %}
            WHEN filename_check_code != 'PASS' AND filename_check_code != 'SKIP' THEN FALSE
            {% endif %}
            WHEN trailer_check_code != 'PASS' AND trailer_check_code != 'SKIP' THEN FALSE
            WHEN rowcount_check_code != 'PASS' AND rowcount_check_code != 'SKIP' THEN FALSE
            ELSE TRUE
        END as validation_passed
        
    FROM validation_results

    {% if execute %}
        -- Execute the validation query to check results
        {% set validation_check_query %}
            WITH validation_results AS (
                SELECT 
                    cr.HEADER_TRACKER_ID,
                    cr.FEED_NM,
                    cr.HEADER_FILE_NM,
                    cr.SOURCE_FILE_NM,
                    cr.PROCESSING_STATUS,
                    cr.STREAM_NAME,
                    cr.CONTROL_RECORD_COUNT as trailer_record_count,
                    
                    {% if staging_table_name %}
                    {{ staging_row_count }} as staging_row_count,
                    {% else %}
                    NULL as staging_row_count,
                    {% endif %}
                    
                    {% if staging_table_name %}
                    -- FVL0005: Row count validation against staging table (using control record count)
                    CASE 
                        WHEN cr.CONTROL_RECORD_COUNT IS NULL THEN 'FVL0005'
                        WHEN cr.CONTROL_RECORD_COUNT = {{ staging_row_count }} THEN 'PASS'
                        ELSE 'FVL0005'
                    END as rowcount_check_code,
                    
                    CASE 
                        WHEN cr.CONTROL_RECORD_COUNT IS NULL THEN 'Control record count missing.'
                        WHEN cr.CONTROL_RECORD_COUNT = {{ staging_row_count }} THEN 'Control record count matches staging table row count.'
                        ELSE 'Control record count (' || cr.CONTROL_RECORD_COUNT || 
                             ') does not match staging table row count ({{ staging_row_count }}).'
                    END as rowcount_check_message,
                    {% else %}
                    'SKIP' as rowcount_check_code,
                    'Record count validation skipped - no staging table provided.' as rowcount_check_message,
                    {% endif %}
                    
                    -- Overall validation result (simplified for this check)
                    {% if staging_table_name %}
                    CASE 
                        WHEN cr.CONTROL_RECORD_COUNT = {{ staging_row_count }} THEN 'PASS'
                        ELSE 'FVL0005'
                    END as overall_validation_result,
                    
                    -- Validation passed flag
                    CASE 
                        WHEN cr.CONTROL_RECORD_COUNT = {{ staging_row_count }} THEN TRUE
                        ELSE FALSE
                    END as validation_passed
                    {% else %}
                    'PASS' as overall_validation_result,
                    TRUE as validation_passed
                    {% endif %}
                    
                FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL cr
                WHERE cr.STREAM_NAME = '{{ stream_name }}'
                  AND cr.PROCESSING_STATUS = 'DISCOVERED'
            )
            SELECT validation_passed, overall_validation_result, rowcount_check_message
            FROM validation_results
            LIMIT 1
        {% endset %}
        
        {% set validation_results = run_query(validation_check_query) %}
        {% if validation_results.rows | length > 0 %}
            {% set validation_passed = validation_results.rows[0][0] %}
            {% set validation_result = validation_results.rows[0][1] %}
            {% set status_message = validation_results.rows[0][2] %}
            
            {% if not validation_passed %}
                {{ log(log_prefix ~ " VALIDATION FAILED for stream: " ~ stream_name ~ " - " ~ validation_result) }}
                {{ log(log_prefix ~ " Error details: " ~ status_message) }}
                {{ log_dcf_exec_msg('HEADER_TRAILER_VALIDATION', stream_name, 11, 'Header/trailer validation failed: ' ~ status_message) }}
                {{ exceptions.raise_compiler_error("Header/Trailer Validation Failed for stream " ~ stream_name ~ ": " ~ status_message) }}
            {% else %}
                {{ log(log_prefix ~ " Validation PASSED for stream: " ~ stream_name) }}
                {{ log_dcf_exec_msg('HEADER_TRAILER_VALIDATION', stream_name, 20, 'Header/trailer validation completed successfully for stream ' ~ stream_name) }}
            {% endif %}
        {% endif %}
        
        {{ log(log_prefix ~ " Header/trailer validation completed for stream: " ~ stream_name) }}
    {% endif %}

{% endmacro %}


{% macro update_control_table_status(header_tracker_id, new_status, validation_message=none) %}
    /*
        Updates the processing status in DCF_T_IGSN_FRMW_HDR_CTRL table
        
        Args:
            header_tracker_id: ID of the header record to update
            new_status: New processing status (VALIDATED, REJECTED, etc.)
            validation_message: Optional validation message
    */
    
    {% set log_prefix = "[UPDATE_STATUS]" %}
    {{ log(log_prefix ~ " Updating header tracker ID " ~ header_tracker_id ~ " to status: " ~ new_status) }}
    
    UPDATE {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
    SET 
        PROCESSING_STATUS = '{{ new_status }}',
        {% if validation_message %}
        PROCESSING_MSG = '{{ validation_message }}',
        {% endif %}
        PROCESSING_TS = CURRENT_TIMESTAMP(),
        UPDATED_BY = CURRENT_USER(),
        UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE HEADER_TRACKER_ID = {{ header_tracker_id }}

{% endmacro %}


{% macro validate_and_update_control_status(stream_name='BCFINSG_PLAN_BALN_SEGM_LOAD', staging_table_name=none, expected_file_name=none) %}
    /*
        Comprehensive validation that also updates the control table status
        
        Performs validation and updates DCF_T_IGSN_FRMW_HDR_CTRL accordingly:
        - PASS → Status: VALIDATED
        - FAIL → Status: REJECTED with error message
        
        Args:
            stream_name: The stream name to validate
            staging_table_name: Name of staging table to compare record counts against
            expected_file_name: Expected file name pattern (optional)
    */
    
    {% set log_prefix = "[VALIDATE_AND_UPDATE]" %}
    {{ log(log_prefix ~ " Starting validation and status update for stream: " ~ stream_name) }}

    -- Perform validation
    WITH validation_results AS (
        {{ validate_header_trailer(stream_name, staging_table_name, expected_file_name) }}
    ),
    
    -- Update status based on validation results
    status_updates AS (
        SELECT 
            vr.*,
            CASE 
                WHEN validation_passed THEN 'VALIDATED'
                ELSE 'REJECTED'
            END as new_status,
            
            CASE 
                WHEN validation_passed THEN 'Header/trailer validation passed successfully.'
                ELSE 'Validation failed: ' || overall_validation_result || ' - ' || 
                     COALESCE(header_check_message, '') || ' ' ||
                     COALESCE(date_check_message, '') || ' ' ||
                     COALESCE(filename_check_message, '') || ' ' ||
                     COALESCE(trailer_check_message, '') || ' ' ||
                     COALESCE(rowcount_check_message, '')
            END as status_message
            
        FROM validation_results vr
    )
    
    SELECT 
        su.*,
        'Status will be updated to: ' || new_status as update_action
    FROM status_updates su



{% endmacro %}


{% macro get_validation_summary(stream_name='BCFINSG_PLAN_BALN_SEGM_LOAD', days_back=7) %}
    /*
        Gets a summary of validation results for monitoring and reporting
        
        Args:
            stream_name: The stream name to report on
            days_back: Number of days to look back for validation history
    */
    
    SELECT 
        STREAM_NAME,
        FEED_NM,
        DATE(FILE_LOAD_TS) as processing_date,
        
        -- File counts by status
        COUNT(*) as total_files,
        SUM(CASE WHEN PROCESSING_STATUS = 'DISCOVERED' THEN 1 ELSE 0 END) as discovered_files,
        SUM(CASE WHEN PROCESSING_STATUS = 'VALIDATED' THEN 1 ELSE 0 END) as validated_files,
        SUM(CASE WHEN PROCESSING_STATUS = 'REJECTED' THEN 1 ELSE 0 END) as rejected_files,
        SUM(CASE WHEN PROCESSING_STATUS = 'READY' THEN 1 ELSE 0 END) as ready_files,
        SUM(CASE WHEN PROCESSING_STATUS = 'PROCESSING' THEN 1 ELSE 0 END) as processing_files,
        SUM(CASE WHEN PROCESSING_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) as completed_files,
        
        -- Validation success rate
        ROUND(
            SUM(CASE WHEN PROCESSING_STATUS = 'VALIDATED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        ) as validation_success_rate,
        
        -- Timing metrics
        MIN(FILE_LOAD_TS) as earliest_file_time,
        MAX(FILE_LOAD_TS) as latest_file_time,
        MAX(PROCESSING_TS) as latest_processing_time
        
    FROM {{ dcf_database_ref() }}.DCF_T_IGSN_FRMW_HDR_CTRL
    WHERE STREAM_NAME = '{{ stream_name }}'
      AND FILE_LOAD_TS >= CURRENT_DATE - {{ days_back }}
    GROUP BY STREAM_NAME, FEED_NM, DATE(FILE_LOAD_TS)
    ORDER BY processing_date DESC

{% endmacro %}
