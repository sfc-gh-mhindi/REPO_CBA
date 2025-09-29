# DMVA Column-Level Masking Policies

## Overview

This feature enables **source-side data masking during export** by applying column-level masking policies stored in the `dmva_column_info` table. Masking occurs during query generation, ensuring sensitive data is masked before export while maintaining referential integrity for joins.

## Key Features

- ✅ **Column-Level Granularity**: Individual masking policy per column
- ✅ **Join-Safe**: Deterministic functions preserve referential integrity
- ✅ **Database-Native**: Uses each source system's native SQL functions
- ✅ **Performance**: Single-pass masking during export (no additional overhead)
- ✅ **Flexible**: Pattern-based or manual policy assignment

## Schema Changes

```sql
-- Add masking policy columns to dmva_column_info table
ALTER TABLE dmva_column_info 
ADD COLUMN masking_policy_type VARCHAR(50),
ADD COLUMN masking_policy_expression VARCHAR(16777216);
```

## Standard Masking Policy Types

### Core Policy Types with Examples

| Policy Type | Expression | Sample Input | Sample Output |
|-------------|------------|--------------|---------------|
| `DETERMINISTIC_HASH` | `HASH(CONCAT(column_name, 'company_salt'))` | `'123-45-6789'` | `'A1B2C3D4E5F6'` |
| `SHOW_LAST_4` | `LPAD(RIGHT(column_name, 4), LENGTH(column_name), 'X')` | `'123-45-6789'` | `'XXXXX-X-6789'` |
| `EMAIL_MASK` | `REGEXP_REPLACE(column_name, '^[^@]+', 'user')` | `'john.doe@company.com'` | `'user@company.com'` |
| `RANGE_BUCKET` | `CASE WHEN column_name < 50000 THEN '<50K' WHEN column_name < 100000 THEN '50K-99K' ELSE '100K+' END` | `75000` | `'50K-99K'` |
| `RANDOM_RANGE_NUMBER` | `FLOOR(RANDOM() * (100000 - 50000) + 50000)` | `75432` | `67891` |
| `RANDOM_RANGE_DATE` | `DATE_ADD('2020-01-01', INTERVAL FLOOR(RANDOM() * 1460) DAY)` | `'2023-05-15'` | `'2022-03-22'` |
| `RANDOM_STRING` | `CONCAT('USER_', LPAD(FLOOR(RANDOM() * 10000), 4, '0'))` | `'john_doe'` | `'USER_3847'` |
| `RANDOM_ADDRESS` | `CONCAT(FLOOR(RANDOM() * 9999 + 1), ' Main St, City, ST')` | `'123 Oak Ave'` | `'4567 Main St, City, ST'` |
| `RANDOM_NAME` | `CASE FLOOR(RANDOM() * 5) WHEN 0 THEN 'John Smith' WHEN 1 THEN 'Jane Doe' ELSE 'Sample User' END` | `'Michael Johnson'` | `'Jane Doe'` |
| `PERCENTAGE_NOISE` | `column_name * (1 + (RANDOM() - 0.5) * 0.1)` | `75000` | `76234` |
| `FIXED_VALUE` | `50000` | `75000` | `50000` |
| `REDACTED` | `'[REDACTED]'` | `'sensitive_info'` | `'[REDACTED]'` |
| `NULL_VALUE` | `NULL` | `'sensitive_data'` | `NULL` |

## Implementation

### 1. Update System Classes

Modify `system_teradata.py` (and other system classes):

```python
def __get_write_nos_columns(self, column: Column) -> str:
    """Apply masking policy if configured for the column"""
    base_expression = column.identifier
    
    # Apply masking expression if present
    if hasattr(column, 'masking_policy_expression') and column.masking_policy_expression:
        # Replace 'column_name' placeholder with actual column identifier  
        base_expression = column.masking_policy_expression.replace('column_name', column.identifier)
    
    return base_expression
```

### 2. Update Column Object Creation

Ensure Column objects include masking policy from metadata:

```python
def get_columns(self, source_columns: list) -> list:
    columns = []
    for col_data in source_columns:
        column = Column({
            **col_data,
            'masking_policy_type': col_data.get('masking_policy_type'),
            'masking_policy_expression': col_data.get('masking_policy_expression')
        })
        columns.append(column)
    return columns
```

## Configuration Examples

### Using Policy Types and Expressions

```sql
-- Hash all PII columns (join-safe)
UPDATE dmva_column_info 
SET masking_policy_type = 'DETERMINISTIC_HASH',
    masking_policy_expression = 'HASH(CONCAT(column_name, ''prod_salt_2024''))',
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) REGEXP '.*SSN.*|.*SOCIAL.*|.*TAX_ID.*';

-- Partial mask sensitive data
UPDATE dmva_column_info
SET masking_policy_type = 'SHOW_LAST_4',
    masking_policy_expression = 'LPAD(RIGHT(column_name, 4), LENGTH(column_name), ''X'')',
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) LIKE '%PHONE%' OR UPPER(column_name) LIKE '%ACCOUNT%';

-- Email masking (preserve domain)
UPDATE dmva_column_info
SET masking_policy_type = 'EMAIL_MASK',
    masking_policy_expression = 'REGEXP_REPLACE(column_name, ''^[^@]+'', ''user'')',
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) LIKE '%EMAIL%';

-- Random salary ranges for testing
UPDATE dmva_column_info
SET masking_policy_type = 'RANDOM_RANGE_NUMBER',
    masking_policy_expression = 'FLOOR(RANDOM() * (150000 - 30000) + 30000)',
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) IN ('SALARY', 'INCOME', 'COMPENSATION');

-- Random names for customer data
UPDATE dmva_column_info
SET masking_policy_type = 'RANDOM_NAME',
    masking_policy_expression = 'CASE FLOOR(RANDOM() * 3) WHEN 0 THEN ''John Smith'' WHEN 1 THEN ''Jane Doe'' ELSE ''Sample User'' END',
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) LIKE '%NAME%' AND UPPER(column_name) NOT LIKE '%COMPANY%';

-- Add percentage noise to financial data
UPDATE dmva_column_info
SET masking_policy_type = 'PERCENTAGE_NOISE',
    masking_policy_expression = 'column_name * (1 + (RANDOM() - 0.5) * 0.05)',  -- ±2.5% noise
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) LIKE '%AMOUNT%' OR UPPER(column_name) LIKE '%BALANCE%';

-- Redact highly sensitive data
UPDATE dmva_column_info
SET masking_policy_type = 'REDACTED',
    masking_policy_expression = '''[REDACTED]''',
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) LIKE '%PASSWORD%' OR UPPER(column_name) LIKE '%SECRET%';
```

### Set Policies for Specific Tables

```sql
-- Apply salary range masking for HR tables
UPDATE dmva_column_info c
SET masking_policy = 'CASE WHEN column_name < 50000 THEN ''<50K''
                           WHEN column_name < 100000 THEN ''50K-99K'' 
                           ELSE ''100K+'' END',
    updated_ts = CURRENT_TIMESTAMP()
FROM dmva_object_info o  
WHERE c.object_id = o.object_id
  AND o.database_name = 'HR_DB'
  AND UPPER(c.column_name) IN ('SALARY', 'COMPENSATION', 'WAGE');
```

### Environment-Specific Policies

```sql
-- Apply masking only for DEV/TEST environments
UPDATE dmva_column_info c
SET masking_policy = 'HASH(CONCAT(column_name, ''dev_salt''))',
    updated_ts = CURRENT_TIMESTAMP()
FROM dmva_object_info o
JOIN dmva_systems s ON o.system_name = s.system_name
WHERE c.object_id = o.object_id
  AND s.system_details:environment::varchar IN ('DEV', 'TEST')
  AND UPPER(c.column_name) LIKE '%PII%';
```

## Management Commands

### View Current Masking Policies

```sql
-- View all columns with masking policies
SELECT 
    s.system_name,
    o.database_name,
    o.schema_name, 
    o.object_name,
    c.column_name,
    c.data_type,
    c.masking_policy
FROM dmva_column_info c
JOIN dmva_object_info o ON c.object_id = o.object_id  
JOIN dmva_systems s ON o.system_name = s.system_name
WHERE c.masking_policy IS NOT NULL
ORDER BY s.system_name, o.database_name, o.schema_name, o.object_name, c.ordinal_position;
```

### Remove Masking Policies

```sql
-- Remove all masking policies for a specific system
UPDATE dmva_column_info c
SET masking_policy = NULL,
    updated_ts = CURRENT_TIMESTAMP()
FROM dmva_object_info o
WHERE c.object_id = o.object_id
  AND o.system_name = 'dev_system';

-- Remove masking for specific columns
UPDATE dmva_column_info 
SET masking_policy = NULL,
    updated_ts = CURRENT_TIMESTAMP()
WHERE UPPER(column_name) IN ('CUSTOMER_ID', 'ORDER_ID');
```

### Test Masking Output

```sql
-- Preview how masking will be applied (Snowflake example)
WITH sample_data AS (
    SELECT 'john.doe@company.com' as email,
           '123-45-6789' as ssn,
           '555-123-4567' as phone,
           75000 as salary
)
SELECT 
    email,
    REGEXP_REPLACE(email, '^[^@]+', 'user') as masked_email,
    ssn,
    HASH(CONCAT(ssn, 'prod_salt_2024')) as masked_ssn,
    phone,
    CONCAT('XXX-XXX-', RIGHT(phone, 4)) as masked_phone,
    salary,
    CASE WHEN salary < 50000 THEN '<50K'
         WHEN salary < 100000 THEN '50K-99K' 
         ELSE '100K+' END as masked_salary
FROM sample_data;
```

## Database-Specific Considerations

### Teradata
- Use `HASHROW()` for deterministic hashing
- Use `REGEXP_REPLACE()` for pattern masking
- Consider `LPAD/RPAD` for padding operations

### Oracle  
- Use `DBMS_CRYPTO.HASH()` for hashing
- Use `REGEXP_REPLACE()` for pattern masking
- Use `LPAD/RPAD` for padding

### PostgreSQL
- Use `MD5()` or `DIGEST()` for hashing
- Use `REGEXP_REPLACE()` for pattern masking
- Use `LPAD/RPAD` for padding

### SQL Server
- Use `HASHBYTES()` for hashing
- Use string functions for pattern masking
- Consider `FORMAT()` for structured output

## Result Example

**Original Export Query:**
```sql
SELECT customer_id, ssn, email, phone, salary 
FROM customer_data;
```

**Masked Export Query:**
```sql  
SELECT 
    customer_id,  -- No policy = no masking
    HASH(CONCAT(ssn, 'prod_salt_2024')) as ssn,
    REGEXP_REPLACE(email, '^[^@]+', 'user') as email,  
    CONCAT('XXX-XXX-', RIGHT(phone, 4)) as phone,
    CASE WHEN salary < 50000 THEN '<50K'
         WHEN salary < 100000 THEN '50K-99K' 
         ELSE '100K+' END as salary
FROM customer_data;
```

## Best Practices

1. **Test Policies**: Always test masking expressions before applying to production
2. **Consistent Salt**: Use consistent salt values across related tables for join integrity
3. **Document Policies**: Maintain documentation of which columns are masked and why
4. **Environment Strategy**: Consider different masking strategies per environment
5. **Performance**: Monitor query performance with complex masking expressions
6. **Backup Policies**: Keep record of original vs masked data for audit purposes

## Troubleshooting

### Policy Not Applied
- Verify `masking_policy` column value is not NULL
- Check Column object creation includes masking_policy field
- Ensure system class implements policy application logic

### Join Issues  
- Use deterministic functions (HASH with consistent salt)
- Test join queries with masked data
- Consider using same salt across related tables

### Performance Issues
- Simplify complex masking expressions
- Consider pre-computed masked values for static data
- Monitor export query execution times
