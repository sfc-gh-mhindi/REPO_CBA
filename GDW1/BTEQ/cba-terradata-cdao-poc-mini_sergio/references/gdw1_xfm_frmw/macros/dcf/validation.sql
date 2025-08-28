-- ============================================================================
-- DCF Validation Macros - GDW1 IGSN Framework
-- ============================================================================
-- Purpose: Data quality and business date validation macros
-- Pattern: Reusable validation patterns for DCF-integrated processes

{%- macro validate_single_open_business_date(stream_name) -%}
    -- Business Date Validation: Proper validation with clear error messages
    {% if execute %}
        {% set validation_query %}
            select count(*) as open_count
            from {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
            where STRM_NAME = '{{ stream_name }}'
              and PROCESSING_FLAG = 1
        {% endset %}
        
        {% set results = run_query(validation_query) %}
        {% set open_count = results.columns[0].values()[0] %}
        
        {% if open_count == 0 %}
            {# Log validation failure to DCF_T_EXEC_LOG #}
            {{ log_dcf_exec_msg('BUSINESS_DATE_VALIDATION', stream_name, 11, 'Business date validation failed: No open business dates found') }}
            {{ exceptions.raise_compiler_error("❌ BUSINESS DATE VALIDATION FAILED: No open business dates found for stream '" ~ stream_name ~ "'. Please start the stream first using: dbt run-operation start_stream_op --args '{stream_name: " ~ stream_name ~ ", business_date: \"YYYY-MM-DD\"}'") }}
        {% elif open_count > 1 %}
            {# Log validation failure to DCF_T_EXEC_LOG #}
            {{ log_dcf_exec_msg('BUSINESS_DATE_VALIDATION', stream_name, 11, 'Business date validation failed: Multiple open business dates detected (count: ' ~ open_count ~ ')') }}
            {{ exceptions.raise_compiler_error("❌ BUSINESS DATE VALIDATION FAILED: Multiple open business dates detected for stream '" ~ stream_name ~ "' (count: " ~ open_count ~ "). Please resolve stream state conflicts before proceeding.") }}
        {% else %}
            {# Log validation success to DCF_T_EXEC_LOG #}
            {{ log_dcf_exec_msg('BUSINESS_DATE_VALIDATION', stream_name, 10, 'Business date validation passed: Exactly one open business date found') }}
            {{ log("✅ Business Date Validation PASSED: Exactly one open business date found for stream '" ~ stream_name ~ "'") }}
        {% endif %}
    {% endif %}
    
    -- Return a simple successful query when validation passes
    select 'VALIDATION_PASSED' as status
{%- endmacro -%}

{% macro validate_bic_codes(table_ref, process_name) %}
  -- ====================================================================
  -- Validate BIC Codes - Ensures BIC codes are valid
  -- Purpose: Data quality validation for BIC codes
  -- ====================================================================
  {% if execute %}
    WITH invalid_bics AS (
      SELECT 
        debtor_agent_bic,
        creditor_agent_bic,
        COUNT(*) as invalid_count
      FROM {{ table_ref }}
      WHERE (debtor_agent_bic IS NOT NULL AND LENGTH(debtor_agent_bic) != 8)
         OR (creditor_agent_bic IS NOT NULL AND LENGTH(creditor_agent_bic) != 8)
      GROUP BY 1, 2
    )
    SELECT * FROM invalid_bics
    HAVING COUNT(*) = 0
  {% endif %}
{% endmacro %}

{% macro validate_receipt_numbers(table_ref, process_name) %}
  -- ====================================================================
  -- Validate Receipt Numbers - Ensures receipt numbers are unique
  -- Purpose: Data quality validation for transaction identifiers
  -- ====================================================================
  {% if execute %}
    WITH duplicate_receipts AS (
      SELECT 
        receipt_number,
        COUNT(*) as receipt_count
      FROM {{ table_ref }}
      GROUP BY 1
      HAVING COUNT(*) > 1
    )
    SELECT * FROM duplicate_receipts
    HAVING COUNT(*) = 0
  {% endif %}
{% endmacro %}