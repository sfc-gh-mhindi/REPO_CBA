-- =====================================================================
-- PERIOD OVERLAPS UDF (Required for BTEQ to Snowflake Conversion)
-- =====================================================================

-- Description: UDF that reproduces the OVERLAPS OPERATOR to compare two or more  
--              period expressions and indicate if all expressions overlap with each other.
-- Parameters:
--      PERIODS: ARRAY of period expressions to be compared (format: 'start_date*end_date')
-- Returns:
--      TRUE if all period expressions overlap, otherwise FALSE.

CREATE OR REPLACE FUNCTION PERIOD_OVERLAPS_UDF(PERIODS ARRAY)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
IMMUTABLE
COMMENT = 'Snowflake equivalent of Teradata OVERLAPS operator for date period comparisons'
AS
$$
function overlaps(period1, period2) {
    return !(period1[0] >= period2[1] || period2[0] >= period1[1]);
}

function splitPeriod(period){
    return period.split('*');
}

function greatest(date1, date2){
    return date1 > date2 ? date1 : date2;
}

function least(date1, date2){
    return date1 < date2 ? date1 : date2;
}

try {
    if ((PERIODS.includes(null))) {
        return false;
    }
    
    var currentPeriod = splitPeriod(PERIODS[0]);
    for (var i = 1; i < PERIODS.length; i++) {
        var nextPeriod = splitPeriod(PERIODS[i]);
        if(!overlaps(currentPeriod, nextPeriod)) {
            return false;
        }
        currentPeriod = [greatest(currentPeriod[0], nextPeriod[0]), least(currentPeriod[1], nextPeriod[1])];
    }
    
    return true;

} catch (error) {
    return false;
}
$$;

-- Usage examples:
-- SELECT PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT('2023-01-01*2023-12-31', '2023-06-01*2024-06-01'));
-- Returns: TRUE (periods overlap)

-- SELECT PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT('2023-01-01*2023-06-01', '2023-07-01*2023-12-31'));
-- Returns: FALSE (periods do not overlap) 