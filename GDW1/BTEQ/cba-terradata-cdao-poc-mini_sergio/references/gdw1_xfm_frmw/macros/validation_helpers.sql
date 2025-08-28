/*
    CSEL4 Data Validation Helper Macros
    
    This file contains macros for implementing the multi-layer validation framework
    as defined in the CSEL4 Data Validation Framework.
*/

/*
    Note: The main header/trailer validation macro has been moved to:
    macros/validate_header_trailer.sql
    
    Use validate_header_trailer() for comprehensive file validation
    against the DCF_T_IGSN_FRMW_HDR_CTRL control table.
*/


{% macro validate_date_format(date_field) %}
    /*
        Validates date field format using DCF UDF (same as CCODS)
        
        Args:
            date_field: The date field to validate
            
        Returns:
            Boolean expression for date format validation using fn_is_valid_dt UDF
    */
    
    {{ dcf_database_ref() }}.fn_is_valid_dt({{ date_field }})

{% endmacro %}
