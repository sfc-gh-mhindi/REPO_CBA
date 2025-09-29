--
/* <sc-function> D_D00_F_GBL_001_STD_0.GenOutputMsg_01 </sc-function> */




replace function D_D00_F_GBL_001_STD_0.GenOutputMsg
(
    i_EventLabel varchar(30)  character set latin
   ,i_SQLSTATE   char(5)      character set latin
   ,i_SQLCode    integer
   ,i_StepId     char(2)      character set latin
   ,i_CustomMsg  varchar(1000) character set latin
)
    returns varchar(8000) character set latin
    specific D_D00_F_GBL_001_STD_0.GenOutputMsg_01
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return ('Event='    ||trim(i_EventLabel)  ||';'||
            'SQLSTATE=' ||i_SQLSTATE          ||';'||
            'SQLCODE='  ||trim(i_SQLCODE)     ||';'||
            'Step_Id='  ||i_StepId            ||';'||
            'CustomMsg='||trim(i_CustomMsg)   ||';');

--
/* <sc-function> D_D00_F_GBL_001_STD_0.GetPredicate </sc-function> */




replace function D_D00_F_GBL_001_STD_0.GetPredicate
(
    i_InputArgName   varchar(30)
   ,i_InputArgType   varchar(50)
   ,i_InputArgValue  varchar(1000)
)
  returns varchar(1000)
  specific D_D00_F_GBL_001_STD_0.GetPredicate
  returns null on null input
  contains sql
  deterministic
  collation invoker
  inline type 1
  return (
            i_InputArgName||' '||
                            -- Build the predicate action first
                            -- LIKE multi-char
                            case when position('%' in i_InputArgValue) <> 0 then
                                    'like '||case when position(',' in i_InputArgValue) <> 0 then
                                                      'any '
                                                  else
                                                      ''
                                              end
                                 -- LIKE single-char
                                 when position('\_' in i_InputArgValue) <> 0 then
                                    'like '||case when position(',' in i_InputArgValue) <> 0 then
                                                      'any '
                                                  else
                                                      ''
                                              end
                                 when position(',' in i_InputArgValue) <> 0 then
                                        'in '
                                 else '= '
                             end

                             ||'('||case when position(',' in i_InputArgValue) <> 0 then
                                    ''''||oreplace(i_InputArgValue,',',''',''')||''''
                                    else
                                         ''''||i_InputArgValue||''''

                                end||')'

                           );

--
/* <sc-function> D_D00_F_GBL_001_STD_0.GetPredicateClauseString </sc-function> */




replace function D_D00_F_GBL_001_STD_0.GetPredicateClause
(
    i_PredicateBoolean varchar(20)
   ,i_ColumnName       varchar(30)
   ,i_InputArgValue    varchar(1000)
)
  -- 
  -- Function: D_D00_F_GBL_001_STD_0.GetPredicateClause
  -- Script: D_D00_F_GBL_001_STD_0.GetPredicateClause.fnc
  --
  -- Description: Library function to determine type of predicate
  --              required in a dynamic SQL generation.
  --
  -- Overload:    String value predicates
  --
  -- Author: Paul Dancer: Teradata Professional Services
  -- Date 23-08-2014
  -- 
  returns varchar(1000)
  specific D_D00_F_GBL_001_STD_0.GetPredicateClauseString
  returns null on null input
  contains sql
  deterministic
  collation invoker
  inline type 1
  return
    (
       -- 
       -- If the i_InputArgValue is empty string or % then effectively there is
       -- no predicate
       -- 
       case when (i_InputArgValue = '%' or i_InputArgValue = '') then
            --''||' /* '||i_ColumnName||': i_InputArgValue was ''%'' or ''''*/'
            ''
        else
            coalesce(trim(i_PredicateBoolean),'')||' '||
            coalesce (
                          -- 
                          -- A valid predicate is specified
                          -- 
                          i_ColumnName||' '||

                          -- 
                          -- Build the predicate action first
                          -- i.e. =, in, like, like any
                          -- 
                          case when position('%' in i_InputArgValue) <> 0 then
                                    'like '
                                  ||case when position(',' in i_InputArgValue) <> 0 then
                                              -- 
                                              -- Multiple values and a LIKE 0:n pattern found
                                              -- 
                                              'any '
                                         else
                                              -- 
                                              -- Single like pattern found
                                              -- 
                                              ''
                                     end

                               when position('\_' in i_InputArgValue) <> 0 then
                                    'like '
                                  ||case when position(',' in i_InputArgValue) <> 0 then
                                                    -- 
                                                    -- Multiple values and single LIKE char pattern
                                                    -- found (needs escaping)
                                                    -- 
                                                    'any '
                                                else
                                                    ''
                                     end

                               when position(',' in i_InputArgValue) <> 0 then
                                     -- 
                                     -- Multiple values and no LIKE pattern
                                     -- 
                                   'in '
                               else
                                  '= '
                           end
                         ||'('
                         ||case when position(',' in i_InputArgValue) <> 0 then
                                  -- 
                                  -- Quote
                                  -- 
                                  ''''||oreplace(i_InputArgValue,',',''',''')||''''
                              else
                                  ''''||i_InputArgValue||''''

                            end
                         ||')'
                         ||
                         -- 
                         -- Add escape if needed
                         -- 
                         case when position('\_' in i_InputArgValue) <> 0 then
                                  ' escape ''\'''
                              else
                                  ''
                          end

                          ,i_ColumnName||' is null')
           end
    );

--
/* <sc-function> D_D00_F_GBL_001_STD_0.GetStdScriptHeader </sc-function> */




replace function D_D00_F_GBL_001_STD_0.GetStdScriptHeader
(
   i_NVPArgs varchar(2048)
)
    -- 
    -- Author     : Paul Dancer (Teradata Professional Services)
    -- Description: Function to return a standard SQL commented header
    --
    -- Pass in name value pairs in the form: '<name1>=value1; <name2>=value2;'
    -- Example execution:
    -- D_D00_F_GBL_001_STD_0.GetXMLScriptHeader('<module>=D_D00_P_GBL_001_STD_0.GenPivot;<Version>=1.0;DDL=Generated code to perfrom some action.');
    -- 
    returns varchar(2048) character set latin
    specific D_D00_F_GBL_001_STD_0.GetStdScriptHeader
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (
                '-- -----------------'||'0d'xc
                '-- Generated at       :'||cast(current_timestamp(0) as varchar(19))  ||'-->'||'0d'xc||
                '-- Generated by       :'||user||'-->'||'0d'xc||
                '-- Generator Module   :'||cast(coalesce(nvp(i_NVPArgs,'<module>',';','=',1),'Not defined')      as varchar(257))||'0d'xc||
                '-- Version            :'||cast(coalesce(nvp(i_NVPArgs,'<version>',';','=',1),'Not defined')     as varchar(20 ))||'0d'xc||
                '-- Description        :'||cast(coalesce(nvp(i_NVPArgs,'DDL',';','=',1),'Not defined') as varchar(255))||'0d'xc||
                '-- -----------------'||'0d'xc
           );

--
/* <sc-function> D_D00_F_GBL_001_STD_0.GetXMLScriptHeader </sc-function> */




replace function D_D00_F_GBL_001_STD_0.GetXMLScriptHeader
(
   i_NVPArgs varchar(2048)
)
    -- 
    -- Author     : Paul Dancer (Teradata Professional Services)
    -- Description: Function to return a standard XML commented header
    -- 
    -- Pass in name value pairs in the form: '<name1>=value1; <name2>=value2;'
    -- Example execution:
    -- D_D00_F_GBL_001_STD_0.GetXMLScriptHeader('<module>=D_D00_P_GBL_001_STD_0.GenPivot;<Version>=1.0;<description>=Generated code to perfrom some action.');
    -- 
    returns varchar(2048) character set latin
    specific D_D00_F_GBL_001_STD_0.GetXMLScriptHeader
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (
                '<!-- #################################################################################################'||'-->'||'0d'xc
                '<!-- Generated at       :'||cast(current_timestamp(0) as varchar(19))  ||'-->'||'0d'xc||
                '<!-- Generated by       :'||user||'-->'||'0d'xc||
                '<!-- Generator Module   :'||cast(coalesce(nvp(i_NVPArgs,'<module>',';','=',1),'Not defined')      as varchar(257))||'-->'||'0d'xc||
                '<!-- Version            :'||cast(coalesce(nvp(i_NVPArgs,'<version>',';','=',1),'Not defined')     as varchar(20 ))||'-->'||'0d'xc||
                '<!-- Description        :'||cast(coalesce(nvp(i_NVPArgs,'<description>',';','=',1),'Not defined') as varchar(255))||'-->'||'0d'xc||
                '<!-- #################################################################################################'||'-->'||'0d'xc
           );

--
/* <sc-function> D_D00_F_GBL_001_STD_0.right_str </sc-function> */




replace function D_D00_F_GBL_001_STD_0.right_str
(
   i_String  varchar(255)
  ,i_NoChars smallint 
)
    returns varchar(255)
    specific D_D00_F_GBL_001_STD_0.right_str
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (substr(i_String, character_length(i_String) - i_NoChars + 1, i_NoChars));

--
/* <sc-function> D_D00_F_TCF_001_STD_0.char2date </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHAR2DATE 
  (input_date_string VARCHAR(255) CHARACTER SET LATIN, 
   default_date DATE) 
 RETURNS DATE 
 SPECIFIC char2date 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getadate!./getadate.c!CS!char2date!./char2date.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.char2time </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHAR2TIME 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_time TIME(6)) 
 RETURNS TIME(6) 
 SPECIFIC char2time 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getatime!./getatime.c!CS!char2time!./char2time.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.char2timestamp </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHAR2TIMESTAMP 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_timestamp TIMESTAMP(6)) 
 RETURNS TIMESTAMP(6) 
 SPECIFIC char2timestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2timestamp!./char2timestamp.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkbyteint </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKBYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkbyteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chknum!./chknum.c!CS!chkbyteint!./chkbyteint.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkdate </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKDATE 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkdate 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdate!./chkdate.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkdecimal1 </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKDECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdec!./chkdec.c!CS!chkdecimal1!./chkdecimal1.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkdecimal16 </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKDECIMAL16 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal16 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal16!./chkdecimal16.c!CS!chkdec16!./chkdec16.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkdecimal2 </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKDECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal2!./chkdecimal2.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkdecimal4 </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKDECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal4!./chkdecimal4.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkdecimal8 </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKDECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal8!./chkdecimal8.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkfloat </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKFLOAT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkfloat 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkfl!./chkfl.c!CS!chkfloat!./chkfloat.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chkinteger </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKINTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkinteger 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkinteger!./chkinteger.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chksmallint </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKSMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS SMALLINT 
 SPECIFIC chksmallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chksmallint!./chksmallint.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chktime </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKTIME 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktime 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktime!./chktime.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.chktimestamp </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.CHKTIMESTAMP 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktimestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktimestamp!./chktimestamp.c'

--
/* <sc-function> D_D00_F_TCF_001_STD_0.hash_md5 </sc-function> */
REPLACE FUNCTION D_D00_F_TCF_001_STD_0.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> D_S00_F_GBL_001_STD_0.GenOutputMsg_01 </sc-function> */
replace function D_S00_F_GBL_001_STD_0.GenOutputMsg
(
    i_EventLabel varchar(30)  character set latin
   ,i_SQLSTATE   char(5)      character set latin
   ,i_SQLCode    integer
   ,i_StepId     char(2)      character set latin
   ,i_CustomMsg  varchar(1000) character set latin
)
    returns varchar(8000) character set latin
    specific D_S00_F_GBL_001_STD_0.GenOutputMsg_01
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return ('Event='    ||trim(i_EventLabel)  ||';'||
            'SQLSTATE=' ||i_SQLSTATE          ||';'||
            'SQLCODE='  ||trim(i_SQLCODE)     ||';'||
            'Step_Id='  ||i_StepId            ||';'||
            'CustomMsg='||trim(i_CustomMsg)   ||';')
;

--
/* <sc-function> D_S00_F_GBL_001_STD_0.GetPredicate </sc-function> */
replace function D_S00_F_GBL_001_STD_0.GetPredicate
(
    i_InputArgName   varchar(30)
   ,i_InputArgType   varchar(50)
   ,i_InputArgValue  varchar(1000)
)
  returns varchar(1000)
  specific D_S00_F_GBL_001_STD_0.GetPredicate
  returns null on null input
  contains sql
  deterministic
  collation invoker
  inline type 1
  return (
            i_InputArgName||' '||
                            -- Build the predicate action first
                            -- LIKE multi-char
                            case when position('%' in i_InputArgValue) <> 0 then
                                    'like '||case when position(',' in i_InputArgValue) <> 0 then
                                                      'any '
                                                  else
                                                      ''
                                              end
                                 -- LIKE single-char
                                 when position('\_' in i_InputArgValue) <> 0 then
                                    'like '||case when position(',' in i_InputArgValue) <> 0 then
                                                      'any '
                                                  else
                                                      ''
                                              end
                                 when position(',' in i_InputArgValue) <> 0 then
                                        'in '
                                 else '= '
                             end

                             ||'('||case when position(',' in i_InputArgValue) <> 0 then
                                    ''''||oreplace(i_InputArgValue,',',''',''')||''''
                                    else
                                         ''''||i_InputArgValue||''''

                                end||')'

                           );

--
/* <sc-function> D_S00_F_GBL_001_STD_0.GetPredicateClauseString </sc-function> */
replace function D_S00_F_GBL_001_STD_0.GetPredicateClause
(
    i_PredicateBoolean varchar(20)
   ,i_ColumnName       varchar(30)
   ,i_InputArgValue    varchar(1000)
)
  -- 
  -- Function: D_S00_F_GBL_001_STD_0.GetPredicateClause
  -- Script: D_S00_F_GBL_001_STD_0.GetPredicateClause.fnc
  --
  -- Description: Library function to determine type of predicate
  --              required in a dynamic SQL generation.
  --
  -- Overload:    String value predicates
  --
  -- Author: Paul Dancer: Teradata Professional Services
  -- Date 23-08-2014
  -- 
  returns varchar(1000)
  specific D_S00_F_GBL_001_STD_0.GetPredicateClauseString
  returns null on null input
  contains sql
  deterministic
  collation invoker
  inline type 1
  return
    (
       -- 
       -- If the i_InputArgValue is empty string or % then effectively there is
       -- no predicate
       -- 
       case when (i_InputArgValue = '%' or i_InputArgValue = '') then
            --''||' /* '||i_ColumnName||': i_InputArgValue was ''%'' or ''''*/'
            ''
        else
            coalesce(trim(i_PredicateBoolean),'')||' '||
            coalesce (
                          -- 
                          -- A valid predicate is specified
                          -- 
                          i_ColumnName||' '||

                          -- 
                          -- Build the predicate action first
                          -- i.e. =, in, like, like any
                          -- 
                          case when position('%' in i_InputArgValue) <> 0 then
                                    'like '
                                  ||case when position(',' in i_InputArgValue) <> 0 then
                                              -- 
                                              -- Multiple values and a LIKE 0:n pattern found
                                              -- 
                                              'any '
                                         else
                                              -- 
                                              -- Single like pattern found
                                              -- 
                                              ''
                                     end

                               when position('\_' in i_InputArgValue) <> 0 then
                                    'like '
                                  ||case when position(',' in i_InputArgValue) <> 0 then
                                                    -- 
                                                    -- Multiple values and single LIKE char pattern
                                                    -- found (needs escaping)
                                                    -- 
                                                    'any '
                                                else
                                                    ''
                                     end

                               when position(',' in i_InputArgValue) <> 0 then
                                     -- 
                                     -- Multiple values and no LIKE pattern
                                     -- 
                                   'in '
                               else
                                  '= '
                           end
                         ||'('
                         ||case when position(',' in i_InputArgValue) <> 0 then
                                  -- 
                                  -- Quote
                                  -- 
                                  ''''||oreplace(i_InputArgValue,',',''',''')||''''
                              else
                                  ''''||i_InputArgValue||''''

                            end
                         ||')'
                         ||
                         -- 
                         -- Add escape if needed
                         -- 
                         case when position('\_' in i_InputArgValue) <> 0 then
                                  ' escape ''\'''
                              else
                                  ''
                          end

                          ,i_ColumnName||' is null')
           end
    );

--
/* <sc-function> D_S00_F_GBL_001_STD_0.GetStdScriptHeader </sc-function> */
replace function D_S00_F_GBL_001_STD_0.GetStdScriptHeader
(
   i_NVPArgs varchar(2048)
)
    -- 
    -- Author     : Paul Dancer (Teradata Professional Services)
    -- Description: Function to return a standard SQL commented header
    --
    -- Pass in name value pairs in the form: '<name1>=value1; <name2>=value2;'
    -- Example execution:
    -- D_S00_F_GBL_001_STD_0.GetXMLScriptHeader('<module>=D_S00_P_GBL_001_STD_0.GenPivot;<Version>=1.0;DDL=Generated code to perfrom some action.');
    -- 
    returns varchar(2048) character set latin
    specific D_S00_F_GBL_001_STD_0.GetStdScriptHeader
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (
                '-- -----------------'||'0d'xc
                '-- Generated at       :'||cast(current_timestamp(0) as varchar(19))  ||'-->'||'0d'xc||
                '-- Generated by       :'||user||'-->'||'0d'xc||
                '-- Generator Module   :'||cast(coalesce(nvp(i_NVPArgs,'<module>',';','=',1),'Not defined')      as varchar(257))||'0d'xc||
                '-- Version            :'||cast(coalesce(nvp(i_NVPArgs,'<version>',';','=',1),'Not defined')     as varchar(20 ))||'0d'xc||
                '-- Description        :'||cast(coalesce(nvp(i_NVPArgs,'DDL',';','=',1),'Not defined') as varchar(255))||'0d'xc||
                '-- -----------------'||'0d'xc
           );

--
/* <sc-function> D_S00_F_GBL_001_STD_0.GetXMLScriptHeader </sc-function> */
replace function D_S00_F_GBL_001_STD_0.GetXMLScriptHeader
(
   i_NVPArgs varchar(2048)
)
    -- 
    -- Author     : Paul Dancer (Teradata Professional Services)
    -- Description: Function to return a standard XML commented header
    -- 
    -- Pass in name value pairs in the form: '<name1>=value1; <name2>=value2;'
    -- Example execution:
    -- D_S00_F_GBL_001_STD_0.GetXMLScriptHeader('<module>=D_S00_P_GBL_001_STD_0.GenPivot;<Version>=1.0;<description>=Generated code to perfrom some action.');
    -- 
    returns varchar(2048) character set latin
    specific D_S00_F_GBL_001_STD_0.GetXMLScriptHeader
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (
                '<!-- #################################################################################################'||'-->'||'0d'xc
                '<!-- Generated at       :'||cast(current_timestamp(0) as varchar(19))  ||'-->'||'0d'xc||
                '<!-- Generated by       :'||user||'-->'||'0d'xc||
                '<!-- Generator Module   :'||cast(coalesce(nvp(i_NVPArgs,'<module>',';','=',1),'Not defined')      as varchar(257))||'-->'||'0d'xc||
                '<!-- Version            :'||cast(coalesce(nvp(i_NVPArgs,'<version>',';','=',1),'Not defined')     as varchar(20 ))||'-->'||'0d'xc||
                '<!-- Description        :'||cast(coalesce(nvp(i_NVPArgs,'<description>',';','=',1),'Not defined') as varchar(255))||'-->'||'0d'xc||
                '<!-- #################################################################################################'||'-->'||'0d'xc
           );

--
/* <sc-function> D_S00_F_GBL_001_STD_0.right_str </sc-function> */
replace function D_S00_F_GBL_001_STD_0.right_str
(
   i_String  varchar(255)
  ,i_NoChars smallint 
)
    returns varchar(255)
    specific D_S00_F_GBL_001_STD_0.right_str
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (substr(i_String, character_length(i_String) - i_NoChars + 1, i_NoChars));

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2bigint </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2BIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_bigint BIGINT) 
 RETURNS BIGINT 
 SPECIFIC char2bigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2bigint!./char2bigint.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2byteint </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2BYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_byteint BYTEINT) 
 RETURNS BYTEINT 
 SPECIFIC char2byteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2byteint!./char2byteint.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2date </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2DATE 
  (input_date_string VARCHAR(255) CHARACTER SET LATIN, 
   default_date DATE) 
 RETURNS DATE 
 SPECIFIC char2date 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getadate!./getadate.c!CS!char2date!./char2date.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2decimal1 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2DECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(2,0)) 
 RETURNS DECIMAL(2,0) 
 SPECIFIC char2decimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal1!./char2decimal1.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2decimal2 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2DECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(4,0)) 
 RETURNS DECIMAL(4,0) 
 SPECIFIC char2decimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal2!./char2decimal2.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2decimal4 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2DECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(9,0)) 
 RETURNS DECIMAL(9,0) 
 SPECIFIC char2decimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal4!./char2decimal4.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2decimal8 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2DECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(18,0)) 
 RETURNS DECIMAL(18,0) 
 SPECIFIC char2decimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal8!./char2decimal8.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2integer </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2INTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_integer INTEGER) 
 RETURNS INTEGER 
 SPECIFIC char2integer 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2integer!./char2integer.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2smallint </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2SMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_smallint SMALLINT) 
 RETURNS SMALLINT 
 SPECIFIC char2smallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2smallint!./char2smallint.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2time </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2TIME 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_time TIME(6)) 
 RETURNS TIME(6) 
 SPECIFIC char2time 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getatime!./getatime.c!CS!char2time!./char2time.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.char2timestamp </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHAR2TIMESTAMP 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_timestamp TIMESTAMP(6)) 
 RETURNS TIMESTAMP(6) 
 SPECIFIC char2timestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2timestamp!./char2timestamp.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkbigint </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKBIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BIGINT 
 SPECIFIC chkbigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkbigint!./chkbigint.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkbyteint </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKBYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkbyteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chknum!./chknum.c!CS!chkbyteint!./chkbyteint.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkdate </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKDATE 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkdate 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdate!./chkdate.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkdecimal1 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKDECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdec!./chkdec.c!CS!chkdecimal1!./chkdecimal1.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkdecimal16 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKDECIMAL16 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal16 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal16!./chkdecimal16.c!CS!chkdec16!./chkdec16.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkdecimal2 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKDECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal2!./chkdecimal2.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkdecimal4 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKDECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal4!./chkdecimal4.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkdecimal8 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKDECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal8!./chkdecimal8.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkfloat </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKFLOAT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkfloat 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkfl!./chkfl.c!CS!chkfloat!./chkfloat.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chkinteger </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKINTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkinteger 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkinteger!./chkinteger.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chksmallint </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKSMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS SMALLINT 
 SPECIFIC chksmallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chksmallint!./chksmallint.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chktime </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKTIME 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktime 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktime!./chktime.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.chktimestamp </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.CHKTIMESTAMP 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktimestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktimestamp!./chktimestamp.c'

--
/* <sc-function> D_S00_F_TCF_001_STD_0.hash_md5 </sc-function> */
REPLACE FUNCTION D_S00_F_TCF_001_STD_0.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> PFGDWPIIOBF.hash_md5 </sc-function> */
REPLACE FUNCTION PFGDWPIIOBF.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> PFGDWPIIOBF.Luhn </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.Luhn (InputString VARCHAR(20))
   RETURNS CHAR(1)
   CONTAINS SQL
   DETERMINISTIC
   RETURNS NULL ON NULL INPUT
   COLLATION INVOKER
   INLINE TYPE 1
   RETURN  CAST(((10 - (
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-0) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-0) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-1) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-2) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-2) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-3) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-4) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-4) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-5) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-6) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-6) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-7) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-8) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-8) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-9) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-10) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-10) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-11) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-12) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-12) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-13) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-14) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-14) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-15) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-16) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-16) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-17) FOR 1) AS BYTEINT)
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-18) FOR 1) AS BYTEINT) / 10
                        + 2 * CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-18) FOR 1) AS BYTEINT) MOD 10
                        +     CAST(SUBSTRING(InputString FROM (CHARACTER_LENGTH(InputString)-19) FOR 1) AS BYTEINT)
                        ) MOD 10) MOD 10) AS CHAR(1));

--
/* <sc-function> PFGDWPIIOBF.MIG_AccountNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_AccountNumberCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b[0-9]{8,16}\b',1,1,0,'i') > 0
                    THEN 'Y'
                    ELSE 'N'
            END;

--
/* <sc-function> PFGDWPIIOBF.MIG_AustralianBusinessNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_AustralianBusinessNumberCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_SIMILAR(InputString,'\b[0-9]{11}\b','i') = 1
                        THEN    CASE
                                    WHEN
                                        (
                                            (CAST(SUBSTRING(InputString FROM 1 FOR 1) AS BYTEINT) - 1) * 10
                                        + CAST(SUBSTRING(InputString FROM 2 FOR 1) AS BYTEINT) * 1
                                        + CAST(SUBSTRING(InputString FROM 3 FOR 1) AS BYTEINT) * 3
                                        + CAST(SUBSTRING(InputString FROM 4 FOR 1) AS BYTEINT) * 5                    
                                        + CAST(SUBSTRING(InputString FROM 5 FOR 1) AS BYTEINT) * 7
                                        + CAST(SUBSTRING(InputString FROM 6 FOR 1) AS BYTEINT) * 9                    
                                        + CAST(SUBSTRING(InputString FROM 7 FOR 1) AS BYTEINT) * 11                    
                                        + CAST(SUBSTRING(InputString FROM 8 FOR 1) AS BYTEINT) * 13                    
                                        + CAST(SUBSTRING(InputString FROM 9 FOR 1) AS BYTEINT) * 15 
                                        + CAST(SUBSTRING(InputString FROM 10 FOR 1) AS BYTEINT) * 17 
                                        + CAST(SUBSTRING(InputString FROM 11 FOR 1) AS BYTEINT) * 19 
                                        ) MOD 89 = 0
                                        THEN 'Y'
                                        ELSE 'N'
                                END
                    ELSE 'N'
            END;

--
/* <sc-function> PFGDWPIIOBF.MIG_AustralianCompanyNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_AustralianCompanyNumberCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_SIMILAR(InputString,'\b[0-9]{9}\b','i') = 1
                        THEN    CASE
                                    WHEN
                    10 -
                                          ((
                                            CAST(SUBSTRING(InputString FROM 1 FOR 1) AS BYTEINT) * 8
                                          + CAST(SUBSTRING(InputString FROM 2 FOR 1) AS BYTEINT) * 7
                                          + CAST(SUBSTRING(InputString FROM 3 FOR 1) AS BYTEINT) * 6
                                          + CAST(SUBSTRING(InputString FROM 4 FOR 1) AS BYTEINT) * 5                    
                                          + CAST(SUBSTRING(InputString FROM 5 FOR 1) AS BYTEINT) * 4
                                          + CAST(SUBSTRING(InputString FROM 6 FOR 1) AS BYTEINT) * 3                    
                                          + CAST(SUBSTRING(InputString FROM 7 FOR 1) AS BYTEINT) * 2                    
                                          + CAST(SUBSTRING(InputString FROM 8 FOR 1) AS BYTEINT) * 1                    
                                          ) MOD 10
                                          ) = CAST(SUBSTRING(InputString FROM 9 FOR 1) AS BYTEINT)
                                        THEN 'Y'
              WHEN
                    (
                                            CAST(SUBSTRING(InputString FROM 1 FOR 1) AS BYTEINT) * 8
                                          + CAST(SUBSTRING(InputString FROM 2 FOR 1) AS BYTEINT) * 7
                                          + CAST(SUBSTRING(InputString FROM 3 FOR 1) AS BYTEINT) * 6
                                          + CAST(SUBSTRING(InputString FROM 4 FOR 1) AS BYTEINT) * 5                    
                                          + CAST(SUBSTRING(InputString FROM 5 FOR 1) AS BYTEINT) * 4
                                          + CAST(SUBSTRING(InputString FROM 6 FOR 1) AS BYTEINT) * 3                    
                                          + CAST(SUBSTRING(InputString FROM 7 FOR 1) AS BYTEINT) * 2                    
                                          + CAST(SUBSTRING(InputString FROM 8 FOR 1) AS BYTEINT) * 1                    
                                          ) MOD 10
                                          = 0 AND CAST(SUBSTRING(InputString FROM 9 FOR 1) AS BYTEINT) = 0
                THEN 'Y'
                                        ELSE 'N'
            END
      ELSE 'N'
            END;

--
/* <sc-function> PFGDWPIIOBF.MIG_CHAREmptyTableCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_CHAREmptyTableCheck (InputString VARCHAR(10))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN 'N';

--
/* <sc-function> PFGDWPIIOBF.MIG_CreditCardNumberCheckNumber </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_CreditCardNumberCheckNumber (InputNumber NUMBER)
     RETURNS CHAR(1)
     CONTAINS SQL
     DETERMINISTIC
     RETURNS NULL ON NULL INPUT
     COLLATION INVOKER
     INLINE TYPE 1
     RETURN  CASE 
               WHEN InputNumber BETWEEN 1000000000000000 AND 9999999999999999
                    AND CAST((InputNumber / 1E10) AS INTEGER) IN (521729,535316,532655,494052,535318,406587,552033,552350,517443,494053)
                    THEN    CASE 
                                   WHEN (
                                        + CAST(InputNumber / 1E0 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E1 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E1 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E2 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E3 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E3 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E4 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E5 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E5 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E6 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E7 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E7 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E8 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E9 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E9 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E10 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E11 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E11 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E12 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E13 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E13 MOD 10 AS BYTEINT) MOD 10
                                        + CAST(InputNumber / 1E14 MOD 10 AS BYTEINT)
                                        + 2 * CAST(InputNumber / 1E15 MOD 10 AS BYTEINT) / 10
                                        + 2 * CAST(InputNumber / 1E15 MOD 10 AS BYTEINT) MOD 10
                                        ) MOD 10 = 0
                                   THEN 'Y'
                                   ELSE 'N'
                              END
                    ELSE 'N'
          END;

--
/* <sc-function> PFGDWPIIOBF.MIG_CreditCardNumberCheckString </sc-function> */

REPLACE FUNCTION PFGDWPIIOBF.MIG_CreditCardNumberCheckString (InputString VARCHAR(16))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'.*((521729|535316|532655|494052|535318|406587|552033|552350|517443|494053)[\s-:|]?([0-9][\s-:|]?){10}).*',1,1,0,'i') > 0
                        THEN    CASE 
                                WHEN (
                                        + CAST(SUBSTRING(InputString FROM 16 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 15 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 15 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 14 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 13 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 13 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 12 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 11 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 11 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 10 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 9 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 9 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 8 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 7 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 7 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 6 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 5 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 5 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 4 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 3 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 3 FOR 1) AS BYTEINT) MOD 10
                                        + CAST(SUBSTRING(InputString FROM 2 FOR 1) AS BYTEINT)
                                        + 2 * CAST(SUBSTRING(InputString FROM 1 FOR 1) AS BYTEINT) / 10
                                        + 2 * CAST(SUBSTRING(InputString FROM 1 FOR 1) AS BYTEINT) MOD 10
                                        ) MOD 10 = 0
                                        THEN 'Y'
                                        ELSE 'N'
                                END
                        ELSE 'N'
                END;

--
/* <sc-function> PFGDWPIIOBF.MIG_DATEEmptyTableCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_DATEEmptyTableCheck (InputString DATE)
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN 'N';

--
/* <sc-function> PFGDWPIIOBF.MIG_DECIMALEmptyTableCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_DECIMALEmptyTableCheck (InputString DECIMAL(37,8))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN 'N';

--
/* <sc-function> PFGDWPIIOBF.MIG_DriversLicenseNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_DriversLicenseNumberCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'^([0-9]){4}([A-Za-z]){2}$',1,1,0,'i') > 0
                    THEN 'Y'
                    ELSE 'N'
            END;

--
/* <sc-function> PFGDWPIIOBF.MIG_EmailAddressCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_EmailAddressCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'([a-z0-9][-a-z0-9_\+\.]*[a-z0-9])@([a-z0-9][-a-z0-9\.]*[a-z0-9]\.(com|edu|gov|info|net|org|au|nz|uk)|([0-9]{1,3}\.{3}[0-9]{1,3}))',1,1,0,'i') > 0
                        AND  lower(InputString) NOT LIKE '%@cba.com.au%' 
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_FreeTextCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_FreeTextCheck (InputString VARCHAR(10000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'.+\s.+\s.+\s.+\s.*',1,1,0,'i')  > 0
                        THEN 'Y'
                        ELSE 'N'
                END;

--
/* <sc-function> PFGDWPIIOBF.MIG_INTEGEREmptyTableCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_INTEGEREmptyTableCheck (InputString BIGINT)
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN 'N';

--
/* <sc-function> PFGDWPIIOBF.MIG_NUMBEREmptyTableCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_NUMBEREmptyTableCheck (InputString NUMBER)
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN 'N';

--
/* <sc-function> PFGDWPIIOBF.MIG_Over50YearDateCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Over50YearDateCheck (InputDate DATE)
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN InputDate <= '1901-01-01' THEN 'N'
                                        WHEN InputDate <= CURRENT_DATE - INTERVAL '50' YEAR 
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_PassportNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_PassportNumberCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b[A-Za-z]{1,2}[0-9]{7}\b',1,1,0,'i') > 0
                        AND TD_SYSFNLIB.REGEXP_INSTR(InputString,'^(o|s|q|i).*',1,1,0,'i') = 0
                        THEN 'Y'
                        ELSE 'N'
                END;

--
/* <sc-function> PFGDWPIIOBF.MIG_PhoneNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_PhoneNumberCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'^(?:\+?(61))? ?(?:\((?=.*\)))?(0?[2-57-8])\)? ?(\d\d(?:[- ](?=\d{3})|(?!\d\d[- ]?\d[- ]))\d\d[- ]?\d[- ]?\d{3})$',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_StaffIDCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_StaffIDCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN  CASE 
                WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b0[0-9]{7}\b',1,1,0,'i') > 0
                    THEN 'Y'
                    ELSE 'N'
            END;

--
/* <sc-function> PFGDWPIIOBF.MIG_StreetAddressCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_StreetAddressCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'[0-9]+\s.+\s(street|st|road|rd|avenue|ave|drive|dr|court|ct|crescent|cr|place|pl|way|close|cl|parade|pde|highway|hwy|circuit|cct|lane|terrace|tce).*',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_TaxFileNumberCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_TaxFileNumberCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN CASE 
                WHEN TD_SYSFNLIB.REGEXP_SIMILAR(InputString,'^(00)?[0-9]{9}$','i') = 1
                    THEN    CASE
                        WHEN    
                            TD_SYSFNLIB.REGEXP_INSTR(InputString,'^[0]{9,11}$',1,1,0,'i') > 0 
                            THEN 'N'
                        WHEN
                            (
                                CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 8 FOR 1) AS BYTEINT) * 1
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 7 FOR 1) AS BYTEINT) * 4
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 6 FOR 1) AS BYTEINT) * 3
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 5 FOR 1) AS BYTEINT) * 7                    
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 4 FOR 1) AS BYTEINT) * 5
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 3 FOR 1) AS BYTEINT) * 8                    
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 2 FOR 1) AS BYTEINT) * 6                    
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 1 FOR 1) AS BYTEINT) * 9                    
                            + CAST(SUBSTRING(InputString FROM CHARACTER_LENGTH(InputString) - 0 FOR 1) AS BYTEINT) * 10 
                            ) MOD 11 = 0
                            THEN 'Y'
                            ELSE 'N'
                        END
                    ELSE 'N'
            END;

--
/* <sc-function> PFGDWPIIOBF.MIG_TIMESTAMPEmptyTableCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_TIMESTAMPEmptyTableCheck (InputString TIMESTAMP(6))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN 'N';

--
/* <sc-function> PFGDWPIIOBF.MIG_Top100FirstNameCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Top100FirstNameCheck (InputString VARCHAR(1000))
    RETURNS CHAR(1)
    CONTAINS SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    COLLATION INVOKER
    INLINE TYPE 1
    RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b(David|John|Peter|Michael|Paul|Andrew|Mark|Robert|Ian|Chris|Steven|James|Tony|Greg|Benjamin|Julie|Richard|Karen|Tim|Jason|Michelle|Stephen|Daniel|Helen|Scott|Sue|Craig|Elizabeth|Matthew|Sarah|William|Simon|Lisa|Anthony|Kate|Thomas|Brian|Gary|Adam|Kim|Geoff|Alan|Rebecca|Matt|Wayne|Jane|Jenny|Susan|Wendy|Amanda|Shane|Anne|Christine|Nick|Darren|Sharon|Bruce|Kevin|Luke|Jennifer|Graham|Fiona|Sam|Brett|Robyn|Margaret|Terry|Emma|Nicole|Phil|Neil|Colin|Melissa|Stuart|Linda|Ken|Jim|Bob|Catherine|Jo|Graeme|Alex|Louise|Brad|Barry|Martin|Trevor|Anna|Kerry|Kylie|Jessica|Ross|Glenn|Debbie|Mary|Nathan|George|Dean|Ray|Angela|TRUST|LTD)\b',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_Top100LastNameCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Top100LastNameCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b(Smith|Jones|Williams|Brown|Wilson|Taylor|Anderson|Johnson|White|Thompson|Lee|Martin|Thomas|Walker|Kelly|Young|Harris|King|Ryan|Roberts|Hall|Evans|Davis|Wright|Baker|Campbell|Edwards|Clark|Robinson|McDonald|Hill|Scott|Clarke|Mitchell|Stewart|Moore|Turner|Miller|Green|Watson|Bell|Wood|Cooper|Murphy|Jackson|James|Lewis|Allen|Bennett|Robertson|Collins|Cook|Murray|Ward|Phillips|O''Brien|Nguyen|Davies|Hughes|Morris|Adams|Johnston|Parker|Ross|Gray|Graham|Russell|Morgan|Reid|Kennedy|Marshall|Singh|Cox|Harrison|Simpson|Richardson|Richards|Carter|Rogers|Walsh|Thomson|Bailey|Matthews|Cameron|Webb|Chapman|Stevens|Ellis|McKenzie|Grant|Shaw|Hunt|Harvey|Butler|Mills|Price|Pearce|Barnes|Henderson|Armstrong|Kumar|TRUST|PTY LTD|LTD)\b',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_Top10CountryNameCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Top10CountryNameCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b(Australia|AU|AUS|China|CN|CHN|United Kingdom|GB|GBR|England|Britain|Great Britain|New Zealand|NZ|NZL|India|IND|United States|US|USA|America|Malaysia|MY|MYS|Republic Of Korea|KR|KOR|South Korea|Germany|DE|DEU|Nepal|NP|NPL|Brazil|BR|BRA)\b',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_Top200LastNameCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Top200LastNameCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b(Smith|Jones|Williams|Brown|Wilson|Taylor|Anderson|Johnson|White|Thompson|Lee|Martin|Thomas|Walker|Kelly|Young|Harris|King|Ryan|Roberts|Hall|Evans|Davis|Wright|Baker|Campbell|Edwards|Clark|Robinson|McDonald|Hill|Scott|Clarke|Mitchell|Stewart|Moore|Turner|Miller|Green|Watson|Bell|Wood|Cooper|Murphy|Jackson|James|Lewis|Allen|Bennett|Robertson|Collins|Cook|Murray|Ward|Phillips|O''Brien|Nguyen|Davies|Hughes|Morris|Adams|Johnston|Parker|Ross|Gray|Graham|Russell|Morgan|Reid|Kennedy|Marshall|Singh|Cox|Harrison|Simpson|Richardson|Richards|Carter|Rogers|Walsh|Thomson|Bailey|Matthews|Cameron|Webb|Chapman|Stevens|Ellis|McKenzie|Grant|Shaw|Hunt|Harvey|Butler|Mills|Price|Pearce|Barnes|Henderson|Armstrong|Fraser|Fisher|Knight|Hamilton|Mason|Hunter|Hayes|Ferguson|Dunn|Wallace|Ford|Elliott|Foster|Gibson|Gordon|Howard|Burns|O''Connor|Jenkins|Woods|Palmer|Reynolds|Holmes|Black|Griffiths|McLean|Day|Andrews|Lloyd|Morrison|West|Duncan|Wang|Sullivan|Rose|Chen|Powell|Brooks|Dawson|MacDonald|Dixon|Wong|Saunders|Watts|Francis|Fletcher|Tran|Rowe|Li|Nelson|Williamson|Lawrence|Porter|Payne|Byrne|FitzGerald|Crawford|Barker|Perry|Hart|Davidson|Wilkinson|Fox|Cole|Lane|Kerr|Lynch|Webster|Pearson|McCarthy|Doyle|Stone|Carroll|Peters|Stephens|Freeman|George|Wells|Alexander|McMahon|Tan|Chan|McGrath|Spencer|May|Lowe|Zhang|Douglas|Coleman|O''Neill|Barrett|Boyd|Burgess|Sutton|Burke|Dean|Atkinson|Patterson|Hogan|Gill|Kumar|TRUST|LTD)\b',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_Top20CountryNameCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Top20CountryNameCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b(Australia|AU|AUS|China|CN|CHN|United Kingdom|GB|GBR|England|Britain|Great Britain|New Zealand|NZ|NZL|India|IN|IND|United States|US|USA|America|Malaysia|MY|MYS|Republic Of Korea|KR|KOR|South Korea|Germany|DE|DEU|Nepal|NP|NPL|Brazil|BR|BRA|Viet Nam|Vietnam|VN|VNM|Indonesia|ID|IDN|Hong Kong|HK|HKG|Italy|IT|ITA|Canada|CA|CAN|Singapore|SG|SGP|Philippines|PH|PHL|Thailand|TH|THA|France|FR|FRA)\b',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFGDWPIIOBF.MIG_Top50FirstNameCheck </sc-function> */



REPLACE FUNCTION PFGDWPIIOBF.MIG_Top50FirstNameCheck (InputString VARCHAR(1000))
        RETURNS CHAR(1)
        CONTAINS SQL
        DETERMINISTIC
        RETURNS NULL ON NULL INPUT
        COLLATION INVOKER
        INLINE TYPE 1
        RETURN CASE  WHEN TD_SYSFNLIB.REGEXP_INSTR(InputString,'\b(David|John|Peter|Michael|Paul|Andrew|Mark|Robert|Ian|Chris|Steven|James|Tony|Greg|Benjamin|Julie|Richard|Karen|Tim|Jason|Michelle|Stephen|Daniel|Helen|Scott|Sue|Craig|Elizabeth|Matthew|Sarah|William|Simon|Lisa|Anthony|Kate|Thomas|Brian|Gary|Adam|Kim|Geoff|Alan|Rebecca|Matt|Wayne|Jane|Jenny|Susan|Wendy|Amanda|TRUST|PTY LTD|LTD)\b',1,1,0,'i') > 0
                        THEN 'Y'
                        ELSE 'N'
                        END;

--
/* <sc-function> PFSECURITY.RSADecrypt </sc-function> */
REPLACE FUNCTION PFSECURITY.RSADECRYPT 
  (EncyptedText VARCHAR(240) CHARACTER SET LATIN, 
   EnvTag VARCHAR(100) CHARACTER SET LATIN) 
 RETURNS VARCHAR(1000) CHARACTER SET LATIN 
 SPECIFIC RSADecrypt 
 LANGUAGE JAVA 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE JAVA 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'jarIdRsaEncrypt:com.teradata.util.ciphers.RSACipher.RSADecrypt(java.lang.String,java.lang.String) returns java.lang.String'

--
/* <sc-function> PFSECURITY.RSAEncrypt </sc-function> */
REPLACE FUNCTION PFSECURITY.RSAENCRYPT 
  (PlanText VARCHAR(240) CHARACTER SET LATIN, 
   EnvTag VARCHAR(100) CHARACTER SET LATIN) 
 RETURNS VARCHAR(1000) CHARACTER SET LATIN 
 SPECIFIC RSAEncrypt 
 LANGUAGE JAVA 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE JAVA 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'jarIdRsaEncrypt:com.teradata.util.ciphers.RSACipher.RSAEncrypt(java.lang.String,java.lang.String) returns java.lang.String'

--
/* <sc-function> PFTCF.char2bigint </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2BIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_bigint BIGINT) 
 RETURNS BIGINT 
 SPECIFIC char2bigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2bigint!./char2bigint.c'

--
/* <sc-function> PFTCF.char2byteint </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2BYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_byteint BYTEINT) 
 RETURNS BYTEINT 
 SPECIFIC char2byteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2byteint!./char2byteint.c'

--
/* <sc-function> PFTCF.char2date </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2DATE 
  (input_date_string VARCHAR(255) CHARACTER SET LATIN, 
   default_date DATE) 
 RETURNS DATE 
 SPECIFIC char2date 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getadate!./getadate.c!CS!char2date!./char2date.c'

--
/* <sc-function> PFTCF.char2decimal1 </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2DECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(2,0)) 
 RETURNS DECIMAL(2,0) 
 SPECIFIC char2decimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal1!./char2decimal1.c'

--
/* <sc-function> PFTCF.char2decimal2 </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2DECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(4,0)) 
 RETURNS DECIMAL(4,0) 
 SPECIFIC char2decimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal2!./char2decimal2.c'

--
/* <sc-function> PFTCF.char2decimal4 </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2DECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(9,0)) 
 RETURNS DECIMAL(9,0) 
 SPECIFIC char2decimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal4!./char2decimal4.c'

--
/* <sc-function> PFTCF.char2decimal8 </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2DECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(18,0)) 
 RETURNS DECIMAL(18,0) 
 SPECIFIC char2decimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal8!./char2decimal8.c'

--
/* <sc-function> PFTCF.char2integer </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2INTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_integer INTEGER) 
 RETURNS INTEGER 
 SPECIFIC char2integer 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2integer!./char2integer.c'

--
/* <sc-function> PFTCF.char2smallint </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2SMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_smallint SMALLINT) 
 RETURNS SMALLINT 
 SPECIFIC char2smallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2smallint!./char2smallint.c'

--
/* <sc-function> PFTCF.char2time </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2TIME 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_time TIME(6)) 
 RETURNS TIME(6) 
 SPECIFIC char2time 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getatime!./getatime.c!CS!char2time!./char2time.c'

--
/* <sc-function> PFTCF.char2timestamp </sc-function> */
REPLACE FUNCTION PFTCF.CHAR2TIMESTAMP 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_timestamp TIMESTAMP(6)) 
 RETURNS TIMESTAMP(6) 
 SPECIFIC char2timestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2timestamp!./char2timestamp.c'

--
/* <sc-function> PFTCF.chkbigint </sc-function> */
REPLACE FUNCTION PFTCF.CHKBIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BIGINT 
 SPECIFIC chkbigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkbigint!./chkbigint.c'

--
/* <sc-function> PFTCF.chkbyteint </sc-function> */
REPLACE FUNCTION PFTCF.CHKBYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkbyteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chknum!./chknum.c!CS!chkbyteint!./chkbyteint.c'

--
/* <sc-function> PFTCF.chkdate </sc-function> */
REPLACE FUNCTION PFTCF.CHKDATE 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkdate 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdate!./chkdate.c'

--
/* <sc-function> PFTCF.chkdecimal1 </sc-function> */
REPLACE FUNCTION PFTCF.CHKDECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdec!./chkdec.c!CS!chkdecimal1!./chkdecimal1.c'

--
/* <sc-function> PFTCF.chkdecimal16 </sc-function> */
REPLACE FUNCTION PFTCF.CHKDECIMAL16 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal16 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal16!./chkdecimal16.c!CS!chkdec16!./chkdec16.c'

--
/* <sc-function> PFTCF.chkdecimal2 </sc-function> */
REPLACE FUNCTION PFTCF.CHKDECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal2!./chkdecimal2.c'

--
/* <sc-function> PFTCF.chkdecimal4 </sc-function> */
REPLACE FUNCTION PFTCF.CHKDECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal4!./chkdecimal4.c'

--
/* <sc-function> PFTCF.chkdecimal8 </sc-function> */
REPLACE FUNCTION PFTCF.CHKDECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal8!./chkdecimal8.c'

--
/* <sc-function> PFTCF.chkfloat </sc-function> */
REPLACE FUNCTION PFTCF.CHKFLOAT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkfloat 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkfl!./chkfl.c!CS!chkfloat!./chkfloat.c'

--
/* <sc-function> PFTCF.chkinteger </sc-function> */
REPLACE FUNCTION PFTCF.CHKINTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkinteger 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkinteger!./chkinteger.c'

--
/* <sc-function> PFTCF.chksmallint </sc-function> */
REPLACE FUNCTION PFTCF.CHKSMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS SMALLINT 
 SPECIFIC chksmallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chksmallint!./chksmallint.c'

--
/* <sc-function> PFTCF.chktime </sc-function> */
REPLACE FUNCTION PFTCF.CHKTIME 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktime 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktime!./chktime.c'

--
/* <sc-function> PFTCF.chktimestamp </sc-function> */
REPLACE FUNCTION PFTCF.CHKTIMESTAMP 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktimestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktimestamp!./chktimestamp.c'

--
/* <sc-function> PFTCF.hash_md5 </sc-function> */
REPLACE FUNCTION PFTCF.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> PFTCF.TCFTransposeColumns </sc-function> */
REPLACE FUNCTION PFTCF.TCFTRANSPOSECOLUMNS 
  (iRowID INTEGER, 
   ColumnNameList VARCHAR(255) CHARACTER SET LATIN, 
   BeforeValueList VARCHAR(255) CHARACTER SET LATIN, 
   AfterValueList VARCHAR(255) CHARACTER SET LATIN, 
   Delimiter VARCHAR(1) CHARACTER SET LATIN) 
 RETURNS TABLE 
  (oRowID INTEGER, 
   ColumnName VARCHAR(40) CHARACTER SET LATIN, 
   BeforeValue VARCHAR(40) CHARACTER SET LATIN, 
   AfterValue VARCHAR(40) CHARACTER SET LATIN)  
 SPECIFIC TCFTransposeColumns 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!TCFTransposeColumns!TCFTransposeColumns.c!F!TCFTransposeColumns'

--
/* <sc-function> PUDF.XOR4BYTE </sc-function> */
REPLACE FUNCTION PUDF.XOR4BYTE 
  (P1 BYTE(4)) 
 RETURNS CHAR(8) CHARACTER SET LATIN 
 CLASS AGGREGATE (60)
 SPECIFIC XOR4BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS;XOR4BYTE;XOR4BYTE.c'

--
/* <sc-function> PUTDBENCH001.ExtractBenchQname </sc-function> */
REPLACE FUNCTION PUTDBENCH001.ExtractBenchQname (BenchInfo VARCHAR(500))
RETURNS VARCHAR(500)
LANGUAGE SQL
CONTAINS SQL
DETERMINISTIC
RETURNS NULL ON NULL INPUT
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN
case 
when BenchInfo like '%exec %benchmark%' or BenchInfo like '%call %benchmark%' then
   regexp_substr(
      regexp_substr(BenchInfo,'(?<=benchmark\.)(.*?)(?=[\s|;|\(])', 1,1,'i'),
   '(.*?)(?=\.|$)')
when BenchInfo like '%/*%tdb=%*/%' then
    regexp_substr(
      regexp_substr(BenchInfo,'(?<=tdb=)(.*?)(?=[\s|\*])', 1,1,'i'),
   '(.*?)(?=\.|$)')
else '' 
end

--
/* <sc-function> PUTDBENCH001.ExtractBenchQSubname </sc-function> */
REPLACE FUNCTION PUTDBENCH001.ExtractBenchQSubname (BenchInfo VARCHAR(500))
RETURNS VARCHAR(500)
LANGUAGE SQL
CONTAINS SQL
DETERMINISTIC
RETURNS NULL ON NULL INPUT
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN
case 
when BenchInfo like '%/*%tdb=%.%*/%' then
    regexp_substr(
      regexp_substr(BenchInfo,'(?<=tdb=)(.*?)(?=[\s|\*])', 1,1,'i'),
   '(?<=\.)(.*?)(?=\*|$)')
else '' 
end

--
/* <sc-function> PUTDBENCH001.TimestampSubtract </sc-function> */
REPLACE FUNCTION PUTDBENCH001.TimestampSubtract (FirstTime TIMESTAMP, SecondTime TIMESTAMP)
RETURNS FLOAT
LANGUAGE SQL
CONTAINS SQL
DETERMINISTIC
RETURNS NULL ON NULL INPUT
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN
	((FirstTime - SecondTime HOUR(4)) (FLOAT)) * 3600. +
	((FirstTime - ((FirstTime - SecondTime HOUR(4) )) - SecondTime SECOND(4) ) (FLOAT))

--
/* <sc-function> P_F_DMO_001_STD_0.HASHMD5 </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.HASHMD5 
  (P1 VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS BYTE(16) 
 SPECIFIC HASHMD5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:./FunctionsFiles/md5.h:cs:md5:./FunctionsFiles/md5.c:cs:md5_byte:./FunctionsFiles/md5_byte.c:F:md5_byte'

--
/* <sc-function> P_F_DMO_001_STD_0.HASHSHA1 </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.HASHSHA1 
  (P1 VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS BYTE(20) 
 SPECIFIC HASHSHA1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'cs:sha1:./FunctionsFiles/sha1.c:cs:sha1_byte:./FunctionsFiles/sha1_byte.c:F:sha1_byte'

--
/* <sc-function> P_F_DMO_001_STD_0.HASHSHA2_256 </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.HASHSHA2_256 
  (P1 VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS BYTE(32) 
 SPECIFIC HASHSHA2_256 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'cs:sha2:./FunctionsFiles/sha256.c:cs:sha256_byte:./FunctionsFiles/sha256_byte.c:F:sha256_byte'

--
/* <sc-function> P_F_DMO_001_STD_0.HASHSHA2_512 </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.HASHSHA2_512 
  (P1 VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS BYTE(64) 
 SPECIFIC HASHSHA2_512 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'cs:sha512:./FunctionsFiles/sha512.c:cs:sha512_byte:./FunctionsFiles/sha512_byte.c:F:sha512_byte'

--
/* <sc-function> P_F_DMO_001_STD_0.XOR16BYTE </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.XOR16BYTE 
  (P1 BYTE(16)) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 CLASS AGGREGATE (64)
 SPECIFIC XOR16BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!XOR16BYTE!./FunctionsFiles/XOR16BYTE.c!F!XOR16BYTE'

--
/* <sc-function> P_F_DMO_001_STD_0.XOR20BYTE </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.XOR20BYTE 
  (P1 BYTE(20)) 
 RETURNS CHAR(40) CHARACTER SET LATIN 
 CLASS AGGREGATE (80)
 SPECIFIC XOR20BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!XOR20BYTE!./FunctionsFiles/XOR20BYTE.c!F!XOR20BYTE'

--
/* <sc-function> P_F_DMO_001_STD_0.XOR32BYTE </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.XOR32BYTE 
  (P1 BYTE(32)) 
 RETURNS CHAR(64) CHARACTER SET LATIN 
 CLASS AGGREGATE (128)
 SPECIFIC XOR32BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!XOR32BYTE!./FunctionsFiles/XOR32BYTE.c!F!XOR32BYTE'

--
/* <sc-function> P_F_DMO_001_STD_0.XOR4BYTE </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.XOR4BYTE 
  (P1 BYTE(4)) 
 RETURNS CHAR(8) CHARACTER SET LATIN 
 CLASS AGGREGATE (60)
 SPECIFIC XOR4BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!XOR4BYTE!./FunctionsFiles/XOR4BYTE.c!F!XOR4BYTE'

--
/* <sc-function> P_F_DMO_001_STD_0.XOR64BYTE </sc-function> */
REPLACE FUNCTION P_F_DMO_001_STD_0.XOR64BYTE 
  (P1 BYTE(64)) 
 RETURNS CHAR(128) CHARACTER SET LATIN 
 CLASS AGGREGATE (256)
 SPECIFIC XOR64BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!XOR64BYTE!./FunctionsFiles/XOR64BYTE.c!F!XOR64BYTE'

--
/* <sc-function> P_F_OAC_001_STD_0.GetReqId </sc-function> */
REPLACE FUNCTION P_F_OAC_001_STD_0.GetReqId (
 i_Prefix VARCHAR(128)
)
RETURNS VARCHAR(128) CHARACTER SET UNICODE
SPECIFIC P_F_OAC_001_STD_0.GetReqId
RETURNS NULL ON NULL INPUT
DETERMINISTIC
CONTAINS SQL
COLLATION INVOKER
INLINE TYPE 1
RETURN (
            -- Prefix
            TRIM(i_Prefix) -- Prefix
         || '_'
            -- Session Number
         ||
     TRIM(CAST( (CAST(SESSION AS INTEGER) (FORMAT '9999999999')) AS CHAR(10))) -- Session
         || '_'
            -- Timestamp
         || 
    CAST(LPAD(TRIM(CAST(
                          (       
                             (CURRENT_DATE - DATE '2000-01-01') * (86400 (BIGINT))
                           + (EXTRACT(HOUR   FROM CURRENT_TIMESTAMP(0) ) * (3600 (BIGINT)))
                           + (EXTRACT(MINUTE FROM CURRENT_TIMESTAMP(0) ) * (60 (BIGINT)))
                           + (EXTRACT(SECOND FROM CURRENT_TIMESTAMP(0) ))
                           )       
                           -- Extra Precision - shift for milliseconds
                           * 100        
                           + CAST(SUBSTR(CAST(CURRENT_TIMESTAMP(2) AS TIMESTAMP(2))(FORMAT 'HH:MI:SS.S(2)') (CHAR(11)), 10,2) AS BYTEINT)
                AS CHAR(11))) ,11,'0') AS CHAR(11))

);

--
/* <sc-function> P_F_OAC_001_STD_0.OAC_Put_Trace_Msg </sc-function> */
REPLACE FUNCTION P_F_OAC_001_STD_0.OAC_PUT_TRACE_MSG 
  (trace_type VARCHAR(30) CHARACTER SET LATIN, 
   trace_ts TIMESTAMP(6), 
   num_01 DECIMAL(38,18), 
   num_02 DECIMAL(38,18), 
   num_03 DECIMAL(38,18), 
   num_04 DECIMAL(38,18), 
   num_05 DECIMAL(38,18), 
   num_06 DECIMAL(38,18), 
   num_07 DECIMAL(38,18), 
   num_08 DECIMAL(38,18), 
   num_09 DECIMAL(38,18), 
   num_10 DECIMAL(38,18), 
   date_01 DATE, 
   date_02 DATE, 
   date_03 DATE, 
   date_04 DATE, 
   date_05 DATE, 
   ts_01 TIMESTAMP(6), 
   ts_02 TIMESTAMP(6), 
   ts_03 TIMESTAMP(6), 
   ts_04 TIMESTAMP(6), 
   ts_05 TIMESTAMP(6), 
   varchar_s_01 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_02 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_03 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_04 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_05 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_06 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_07 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_08 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_09 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_10 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_l_01 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_02 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_03 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_04 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_05 VARCHAR(4095) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC OAC_Put_Trace_Msg 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!OAC_Put_Trace_Msg!OAC_Put_Trace_Msg.c!F!OAC_Put_Trace_Msg'

--
/* <sc-function> P_F_OMR_001_STD_0.Get4DigitYearDate </sc-function> */
REPLACE FUNCTION P_F_OMR_001_STD_0.Get4DigitYearDate
(
    i_2_digit_year_date varchar(8),
    i_century_break     byteint,
    i_date_format       varchar(8)
)
  RETURNS DATE
  SPECIFIC P_F_OMR_001_STD_0.Get4DigitYearDate
  RETURNS NULL ON NULL INPUT
  DETERMINISTIC
  CONTAINS SQL
  COLLATION INVOKER
  INLINE TYPE 1
  RETURN (
           cast(
           case when i_date_format = 'MM-DD-YY' then
                   cast(i_2_digit_year_date as date format 'MM-DD-YY')
                  + case when cast(substring(i_2_digit_year_date from 7 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
                when i_date_format = 'MM/DD/YY' then
                   cast(i_2_digit_year_date as date format 'MM/DD/YY')
                  + case when cast(substring(i_2_digit_year_date from 7 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
                    when i_date_format = 'DD-MM-YY' then
                   cast(i_2_digit_year_date as date format 'DD-MM-YY')
                  + case when cast(substring(i_2_digit_year_date from 7 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
               when i_date_format = 'DD/MM/YY' then
                   cast(i_2_digit_year_date as date format 'DD/MM/YY')
                  + case when cast(substring(i_2_digit_year_date from 7 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
               when i_date_format = 'YY-MM-DD' then
                   cast(i_2_digit_year_date as date format 'YY-MM-DD')
                  + case when cast(substring(i_2_digit_year_date from 1 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
               when i_date_format = 'YY-DD-MM' then
                   cast(i_2_digit_year_date as date format 'YY-DD-MM')
                  + case when cast(substring(i_2_digit_year_date from 1 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END

               when i_date_format = 'YY/MM/DD' then
                   cast(i_2_digit_year_date as date format 'YY/MM/DD')
                  + case when cast(substring(i_2_digit_year_date from 1 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
               when i_date_format = 'YY/DD/MM' then
                   cast(i_2_digit_year_date as date format 'YY/DD/MM')
                  + case when cast(substring(i_2_digit_year_date from 1 for 2) as smallint) <= i_century_break THEN INTERVAL'100' YEAR ELSE INTERVAL'0' YEAR END
           end
           as date)
);

--
/* <sc-function> P_F_OMR_001_STD_0.GetNormalisedDataSize </sc-function> */
REPLACE FUNCTION P_F_OMR_001_STD_0.GetNormalisedDataSize
(
    i_dsc_data_size varchar(20)
)
  RETURNS BIGINT
  SPECIFIC P_F_OMR_001_STD_0.GetNormalisedDataSize
  RETURNS NULL ON NULL INPUT
  DETERMINISTIC
  CONTAINS SQL
  COLLATION INVOKER
  INLINE TYPE 1
  RETURN (
           -- Returns input value in Bytes per second
           -- For Storage 1KB is defined as 1000  bytes - https://en.wikipedia.org/wiki/Kilobyte
           -- Expected input examples:
           -- '1 TB'
           -- '100 GB/s'
           -- '0 B'
           -- '120 KB/s'
           -- 120 KB'
           -- '20 MB/s'
           -- '10 PB/s'
           cast(
           oreplace(
           oreplace(
           oreplace(
           oreplace(
           oreplace(
           oreplace(
           oreplace(i_dsc_data_size,'/s','')
           ,' PB','E15')
           ,' TB','E12')
           ,' GB','E9')
           ,' MB','E6')
           ,' KB','E3')
           ,' B','')
           as bigint)
);

--
/* <sc-function> P_F_OMR_001_STD_0.GetReqId </sc-function> */
REPLACE FUNCTION P_F_OMR_001_STD_0.GetReqId
(
    i_Prefix varchar(3)
)
  RETURNS varchar(29)
  SPECIFIC P_F_OMR_001_STD_0.GetReqId
  RETURNS NULL ON NULL INPUT
  DETERMINISTIC
  CONTAINS SQL
  COLLATION INVOKER
  INLINE TYPE 1
  RETURN (
     -- Prefix
     CAST(LPAD(TRIM(i_Prefix),3,'_') AS char(3)) -- Prefix
  || '_'
     -- Session Number
  || TRIM(CAST( (CAST(SESSION AS integer) (FORMAT '9(11)')) AS char(11))) -- Session
  || '_'
     -- Timestamp
  || CAST(
 (
  (
   (
      (CURRENT_DATE - DATE '2000-01-01') * (86400 (bigint))
    + (EXTRACT(HOUR   FROM CURRENT_TIMESTAMP(0) ) * (3600 (bigint)))
    + (EXTRACT(MINUTE FROM CURRENT_TIMESTAMP(0) ) * (60 (bigint)))
    + (EXTRACT(SECOND FROM CURRENT_TIMESTAMP(0)) (bigint))
    * 100 (bigint)
   )
   + SUBSTR(
     CAST(CAST(CURRENT_TIMESTAMP(2) AS TIMESTAMP(2) ) AS CHAR(25))
    ,POSITION('.' IN CAST(CAST(CURRENT_TIMESTAMP(2) AS TIMESTAMP(2) ) AS CHAR(25)))+1, 2)
  )
 (FORMAT '9(11)'))
 AS CHAR(11))
);

--
/* <sc-function> P_F_OMR_001_STD_0.OMR_Put_Trace_Msg </sc-function> */
REPLACE FUNCTION P_F_OMR_001_STD_0.OMR_PUT_TRACE_MSG 
  (trace_type VARCHAR(30) CHARACTER SET LATIN, 
   trace_ts TIMESTAMP(6), 
   num_01 DECIMAL(38,18), 
   num_02 DECIMAL(38,18), 
   num_03 DECIMAL(38,18), 
   num_04 DECIMAL(38,18), 
   num_05 DECIMAL(38,18), 
   num_06 DECIMAL(38,18), 
   num_07 DECIMAL(38,18), 
   num_08 DECIMAL(38,18), 
   num_09 DECIMAL(38,18), 
   num_10 DECIMAL(38,18), 
   date_01 DATE, 
   date_02 DATE, 
   date_03 DATE, 
   date_04 DATE, 
   date_05 DATE, 
   ts_01 TIMESTAMP(6), 
   ts_02 TIMESTAMP(6), 
   ts_03 TIMESTAMP(6), 
   ts_04 TIMESTAMP(6), 
   ts_05 TIMESTAMP(6), 
   varchar_s_01 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_02 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_03 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_04 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_05 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_06 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_07 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_08 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_09 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_10 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_l_01 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_02 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_03 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_04 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_05 VARCHAR(4095) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC OMR_Put_Trace_Msg 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!OMR_Put_Trace_Msg!OMR_Put_Trace_Msg.c!F!OMR_Put_Trace_Msg'

--
/* <sc-function> P_F_TDS_001_STD_0.GetReqId </sc-function> */
REPLACE FUNCTION P_F_TDS_001_STD_0.GetReqId
(
    i_Prefix VARCHAR(6)
)
  RETURNS VARCHAR(29)
  SPECIFIC P_F_TDS_001_STD_0.GetReqId
  RETURNS NULL ON NULL INPUT
  DETERMINISTIC
  CONTAINS SQL
  COLLATION INVOKER
  INLINE TYPE 1
  RETURN (
            -- Prefix
            CAST(LPAD(TRIM(i_Prefix),6,'_') AS CHAR(6)) -- Prefix
         || '_'
            -- Session Number
         || TRIM(CAST( (CAST(SESSION AS INTEGER) (FORMAT '9999999999')) AS CHAR(10))) -- Session
         || '_'
            -- Timestamp
         || CAST(LPAD(
                TRIM((CAST( CURRENT_TIMESTAMP(0) AS DATE) - DATE '2000-01-01') * (86400 (BIGINT))
                           + (EXTRACT(HOUR   FROM CURRENT_TIMESTAMP(0) ) * (3600 (BIGINT)))
                           + (EXTRACT(MINUTE FROM CURRENT_TIMESTAMP(0) ) * (60 (BIGINT)))
                           + (EXTRACT(SECOND FROM CURRENT_TIMESTAMP(0) ))
                           -- Extra Precision
                           + SUBSTR(CAST(CURRENT_TIMESTAMP(2) AS TIMESTAMP(2) )(FORMAT 'HH:MI:SS.S(2)') (char(11)),10,2)
              )
              ,11,'0') AS CHAR(11))
);

--
/* <sc-function> P_F_TDS_001_STD_0.TDS_Put_Trace_Msg </sc-function> */
REPLACE FUNCTION P_F_TDS_001_STD_0.TDS_PUT_TRACE_MSG 
  (trace_type VARCHAR(30) CHARACTER SET LATIN, 
   trace_ts TIMESTAMP(6), 
   num_01 DECIMAL(38,18), 
   num_02 DECIMAL(38,18), 
   num_03 DECIMAL(38,18), 
   num_04 DECIMAL(38,18), 
   num_05 DECIMAL(38,18), 
   num_06 DECIMAL(38,18), 
   num_07 DECIMAL(38,18), 
   num_08 DECIMAL(38,18), 
   num_09 DECIMAL(38,18), 
   num_10 DECIMAL(38,18), 
   date_01 DATE, 
   date_02 DATE, 
   date_03 DATE, 
   date_04 DATE, 
   date_05 DATE, 
   ts_01 TIMESTAMP(6), 
   ts_02 TIMESTAMP(6), 
   ts_03 TIMESTAMP(6), 
   ts_04 TIMESTAMP(6), 
   ts_05 TIMESTAMP(6), 
   varchar_s_01 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_02 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_03 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_04 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_05 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_06 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_07 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_08 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_09 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_10 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_l_01 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_02 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_03 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_04 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_05 VARCHAR(4095) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC TDS_Put_Trace_Msg 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!TDS_Put_Trace_Msg!TDS_Put_Trace_Msg.c!F!TDS_Put_Trace_Msg'

--
/* <sc-function> P_F_TDS_001_STD_0.XOR4BYTE </sc-function> */
REPLACE FUNCTION P_F_TDS_001_STD_0.XOR4BYTE 
  (P1 BYTE(4)) 
 RETURNS CHAR(8) CHARACTER SET LATIN 
 CLASS AGGREGATE (60)
 SPECIFIC XOR4BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS;XOR4BYTE;XOR4BYTE.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2bigint </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2BIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_bigint BIGINT) 
 RETURNS BIGINT 
 SPECIFIC char2bigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2bigint!./char2bigint.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2byteint </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2BYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_byteint BYTEINT) 
 RETURNS BYTEINT 
 SPECIFIC char2byteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2byteint!./char2byteint.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2date </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2DATE 
  (input_date_string VARCHAR(255) CHARACTER SET LATIN, 
   default_date DATE) 
 RETURNS DATE 
 SPECIFIC char2date 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getadate!./getadate.c!CS!char2date!./char2date.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2decimal1 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2DECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(2,0)) 
 RETURNS DECIMAL(2,0) 
 SPECIFIC char2decimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal1!./char2decimal1.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2decimal2 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2DECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(4,0)) 
 RETURNS DECIMAL(4,0) 
 SPECIFIC char2decimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal2!./char2decimal2.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2decimal4 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2DECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(9,0)) 
 RETURNS DECIMAL(9,0) 
 SPECIFIC char2decimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal4!./char2decimal4.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2decimal8 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2DECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(18,0)) 
 RETURNS DECIMAL(18,0) 
 SPECIFIC char2decimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal8!./char2decimal8.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2integer </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2INTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_integer INTEGER) 
 RETURNS INTEGER 
 SPECIFIC char2integer 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2integer!./char2integer.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2smallint </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2SMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_smallint SMALLINT) 
 RETURNS SMALLINT 
 SPECIFIC char2smallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2smallint!./char2smallint.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2time </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2TIME 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_time TIME(6)) 
 RETURNS TIME(6) 
 SPECIFIC char2time 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getatime!./getatime.c!CS!char2time!./char2time.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.char2timestamp </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHAR2TIMESTAMP 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_timestamp TIMESTAMP(6)) 
 RETURNS TIMESTAMP(6) 
 SPECIFIC char2timestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2timestamp!./char2timestamp.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkbigint </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKBIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BIGINT 
 SPECIFIC chkbigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkbigint!./chkbigint.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkbyteint </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKBYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkbyteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chknum!./chknum.c!CS!chkbyteint!./chkbyteint.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkdate </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKDATE 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkdate 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdate!./chkdate.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkdecimal1 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKDECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdec!./chkdec.c!CS!chkdecimal1!./chkdecimal1.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkdecimal16 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKDECIMAL16 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal16 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal16!./chkdecimal16.c!CS!chkdec16!./chkdec16.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkdecimal2 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKDECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal2!./chkdecimal2.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkdecimal4 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKDECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal4!./chkdecimal4.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkdecimal8 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKDECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal8!./chkdecimal8.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkfloat </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKFLOAT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkfloat 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkfl!./chkfl.c!CS!chkfloat!./chkfloat.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chkinteger </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKINTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkinteger 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkinteger!./chkinteger.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chksmallint </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKSMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS SMALLINT 
 SPECIFIC chksmallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chksmallint!./chksmallint.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chktime </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKTIME 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktime 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktime!./chktime.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.chktimestamp </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.CHKTIMESTAMP 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktimestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktimestamp!./chktimestamp.c'

--
/* <sc-function> P_P00_F_TCF_001_STD_0.hash_md5 </sc-function> */
REPLACE FUNCTION P_P00_F_TCF_001_STD_0.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> P_P01_F_GBL_001_STD_0.GenOutputMsg_01 </sc-function> */




replace function P_P01_F_GBL_001_STD_0.GenOutputMsg
(
    i_EventLabel varchar(30)  character set latin
   ,i_SQLSTATE   char(5)      character set latin
   ,i_SQLCode    integer
   ,i_StepId     char(2)      character set latin
   ,i_CustomMsg  varchar(1000) character set latin
)
    returns varchar(8000) character set latin
    specific P_P01_F_GBL_001_STD_0.GenOutputMsg_01
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return ('Event='    ||trim(i_EventLabel)  ||';'||
            'SQLSTATE=' ||i_SQLSTATE          ||';'||
            'SQLCODE='  ||trim(i_SQLCODE)     ||';'||
            'Step_Id='  ||i_StepId            ||';'||
            'CustomMsg='||trim(i_CustomMsg)   ||';');

--
/* <sc-function> P_P01_F_GBL_001_STD_0.GetPredicate </sc-function> */




replace function P_P01_F_GBL_001_STD_0.GetPredicate
(
    i_InputArgName   varchar(30)
   ,i_InputArgType   varchar(50)
   ,i_InputArgValue  varchar(1000)
)
  returns varchar(1000)
  specific P_P01_F_GBL_001_STD_0.GetPredicate
  returns null on null input
  contains sql
  deterministic
  collation invoker
  inline type 1
  return (
            i_InputArgName||' '||
                            -- Build the predicate action first
                            -- LIKE multi-char
                            case when position('%' in i_InputArgValue) <> 0 then
                                    'like '||case when position(',' in i_InputArgValue) <> 0 then
                                                      'any '
                                                  else
                                                      ''
                                              end
                                 -- LIKE single-char
                                 when position('\_' in i_InputArgValue) <> 0 then
                                    'like '||case when position(',' in i_InputArgValue) <> 0 then
                                                      'any '
                                                  else
                                                      ''
                                              end
                                 when position(',' in i_InputArgValue) <> 0 then
                                        'in '
                                 else '= '
                             end

                             ||'('||case when position(',' in i_InputArgValue) <> 0 then
                                    ''''||oreplace(i_InputArgValue,',',''',''')||''''
                                    else
                                         ''''||i_InputArgValue||''''

                                end||')'

                           );

--
/* <sc-function> P_P01_F_GBL_001_STD_0.GetPredicateClauseString </sc-function> */




replace function P_P01_F_GBL_001_STD_0.GetPredicateClause
(
    i_PredicateBoolean varchar(20)
   ,i_ColumnName       varchar(30)
   ,i_InputArgValue    varchar(1000)
)
  -- 
  -- Function: P_P01_F_GBL_001_STD_0.GetPredicateClause
  -- Script: P_P01_F_GBL_001_STD_0.GetPredicateClause.fnc
  --
  -- Description: Library function to determine type of predicate
  --              required in a dynamic SQL generation.
  --
  -- Overload:    String value predicates
  --
  -- Author: Paul Dancer: Teradata Professional Services
  -- Date 23-08-2014
  -- 
  returns varchar(1000)
  specific P_P01_F_GBL_001_STD_0.GetPredicateClauseString
  returns null on null input
  contains sql
  deterministic
  collation invoker
  inline type 1
  return
    (
       -- 
       -- If the i_InputArgValue is empty string or % then effectively there is
       -- no predicate
       -- 
       case when (i_InputArgValue = '%' or i_InputArgValue = '') then
            --''||' /* '||i_ColumnName||': i_InputArgValue was ''%'' or ''''*/'
            ''
        else
            coalesce(trim(i_PredicateBoolean),'')||' '||
            coalesce (
                          -- 
                          -- A valid predicate is specified
                          -- 
                          i_ColumnName||' '||

                          -- 
                          -- Build the predicate action first
                          -- i.e. =, in, like, like any
                          -- 
                          case when position('%' in i_InputArgValue) <> 0 then
                                    'like '
                                  ||case when position(',' in i_InputArgValue) <> 0 then
                                              -- 
                                              -- Multiple values and a LIKE 0:n pattern found
                                              -- 
                                              'any '
                                         else
                                              -- 
                                              -- Single like pattern found
                                              -- 
                                              ''
                                     end

                               when position('\_' in i_InputArgValue) <> 0 then
                                    'like '
                                  ||case when position(',' in i_InputArgValue) <> 0 then
                                                    -- 
                                                    -- Multiple values and single LIKE char pattern
                                                    -- found (needs escaping)
                                                    -- 
                                                    'any '
                                                else
                                                    ''
                                     end

                               when position(',' in i_InputArgValue) <> 0 then
                                     -- 
                                     -- Multiple values and no LIKE pattern
                                     -- 
                                   'in '
                               else
                                  '= '
                           end
                         ||'('
                         ||case when position(',' in i_InputArgValue) <> 0 then
                                  -- 
                                  -- Quote
                                  -- 
                                  ''''||oreplace(i_InputArgValue,',',''',''')||''''
                              else
                                  ''''||i_InputArgValue||''''

                            end
                         ||')'
                         ||
                         -- 
                         -- Add escape if needed
                         -- 
                         case when position('\_' in i_InputArgValue) <> 0 then
                                  ' escape ''\'''
                              else
                                  ''
                          end

                          ,i_ColumnName||' is null')
           end
    );

--
/* <sc-function> P_P01_F_GBL_001_STD_0.GetStdScriptHeader </sc-function> */




replace function P_P01_F_GBL_001_STD_0.GetStdScriptHeader
(
   i_NVPArgs varchar(2048)
)
    -- 
    -- Author     : Paul Dancer (Teradata Professional Services)
    -- Description: Function to return a standard SQL commented header
    --
    -- Pass in name value pairs in the form: '<name1>=value1; <name2>=value2;'
    -- Example execution:
    -- P_P01_F_GBL_001_STD_0.GetXMLScriptHeader('<module>=P_P00_P_GBL_001_STD_0.GenPivot;<Version>=1.0;DDL=Generated code to perfrom some action.');
    -- 
    returns varchar(2048) character set latin
    specific P_P01_F_GBL_001_STD_0.GetStdScriptHeader
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (
                '-- -----------------'||'0d'xc
                '-- Generated at       :'||cast(current_timestamp(0) as varchar(19))  ||'-->'||'0d'xc||
                '-- Generated by       :'||user||'-->'||'0d'xc||
                '-- Generator Module   :'||cast(coalesce(nvp(i_NVPArgs,'<module>',';','=',1),'Not defined')      as varchar(257))||'0d'xc||
                '-- Version            :'||cast(coalesce(nvp(i_NVPArgs,'<version>',';','=',1),'Not defined')     as varchar(20 ))||'0d'xc||
                '-- Description        :'||cast(coalesce(nvp(i_NVPArgs,'DDL',';','=',1),'Not defined') as varchar(255))||'0d'xc||
                '-- -----------------'||'0d'xc
           );

--
/* <sc-function> P_P01_F_GBL_001_STD_0.GetXMLScriptHeader </sc-function> */




replace function P_P01_F_GBL_001_STD_0.GetXMLScriptHeader
(
   i_NVPArgs varchar(2048)
)
    -- 
    -- Author     : Paul Dancer (Teradata Professional Services)
    -- Description: Function to return a standard XML commented header
    -- 
    -- Pass in name value pairs in the form: '<name1>=value1; <name2>=value2;'
    -- Example execution:
    -- P_P01_F_GBL_001_STD_0.GetXMLScriptHeader('<module>=P_P00_P_GBL_001_STD_0.GenPivot;<Version>=1.0;<description>=Generated code to perfrom some action.');
    -- 
    returns varchar(2048) character set latin
    specific P_P01_F_GBL_001_STD_0.GetXMLScriptHeader
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (
                '<!-- #################################################################################################'||'-->'||'0d'xc
                '<!-- Generated at       :'||cast(current_timestamp(0) as varchar(19))  ||'-->'||'0d'xc||
                '<!-- Generated by       :'||user||'-->'||'0d'xc||
                '<!-- Generator Module   :'||cast(coalesce(nvp(i_NVPArgs,'<module>',';','=',1),'Not defined')      as varchar(257))||'-->'||'0d'xc||
                '<!-- Version            :'||cast(coalesce(nvp(i_NVPArgs,'<version>',';','=',1),'Not defined')     as varchar(20 ))||'-->'||'0d'xc||
                '<!-- Description        :'||cast(coalesce(nvp(i_NVPArgs,'<description>',';','=',1),'Not defined') as varchar(255))||'-->'||'0d'xc||
                '<!-- #################################################################################################'||'-->'||'0d'xc
           );

--
/* <sc-function> P_P01_F_GBL_001_STD_0.right_str </sc-function> */




replace function P_P01_F_GBL_001_STD_0.right_str
(
   i_String  varchar(255)
  ,i_NoChars smallint 
)
    returns varchar(255)
    specific P_P01_F_GBL_001_STD_0.right_str
    returns null on null input
    contains sql
    deterministic
    collation invoker
    inline type 1
    return (substr(i_String, character_length(i_String) - i_NoChars + 1, i_NoChars));

--
/* <sc-function> TUDF.combinedp_u </sc-function> */
REPLACE FUNCTION TUDF.COMBINEDP_U 
  (RSAC INTEGER, 
   inputprofile VARCHAR(127) CHARACTER SET UNICODE) 
 RETURNS CHAR(1) CHARACTER SET LATIN 
 SPECIFIC combinedp_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!combinedp_u!D:\Documents and Settings\chongmi\Desktop\UDF\rowsec\combinedp_u.c'

--
/* <sc-function> TUDF.combinedx_u </sc-function> */
REPLACE FUNCTION TUDF.COMBINEDX_U 
  (RSAC INTEGER, 
   inputprofile VARCHAR(127) CHARACTER SET UNICODE) 
 RETURNS CHAR(1) CHARACTER SET LATIN 
 SPECIFIC combinedx_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!combinedx_u!D:\Documents and Settings\chongmi\Desktop\UDF\rowsec\combinedx_u.c'

--
/* <sc-function> TUDF.combined_u </sc-function> */
REPLACE FUNCTION TUDF.COMBINED_U 
  (RSAC INTEGER, 
   inputprofile VARCHAR(127) CHARACTER SET UNICODE) 
 RETURNS CHAR(1) CHARACTER SET LATIN 
 SPECIFIC combined_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!combined_u!D:\Documents and Settings\chongmi\Desktop\UDF\rowsec\combined_u.c'

--
/* <sc-function> TUDF.digramm_u </sc-function> */
REPLACE FUNCTION TUDF.DIGRAMM_U 
  (strexp1 VARCHAR(256) CHARACTER SET LATIN, 
   strexp2 VARCHAR(256) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC digramm_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 RETURNS NULL ON NULL INPUT 
 EXTERNAL NAME 'CS!digramm_u!D:\Documents and Settings\chongmi\Desktop\UDF\digramm_u.c'

--
/* <sc-function> TUDF.ngramm_u </sc-function> */
REPLACE FUNCTION TUDF.NGRAMM_U 
  (strexp1 VARCHAR(256) CHARACTER SET LATIN, 
   strexp2 VARCHAR(256) CHARACTER SET LATIN, 
   len INTEGER) 
 RETURNS INTEGER 
 SPECIFIC ngramm_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 RETURNS NULL ON NULL INPUT 
 EXTERNAL NAME 'CS!ngramm_u!D:\Documents and Settings\chongmi\Desktop\UDF\ngramm_u.c'

--
/* <sc-function> TUDF.ngramm_u_pos </sc-function> */
REPLACE FUNCTION TUDF.NGRAMM_U_POS 
  (strexp1 VARCHAR(256) CHARACTER SET LATIN, 
   strexp2 VARCHAR(256) CHARACTER SET LATIN, 
   len INTEGER, 
   pos INTEGER) 
 RETURNS INTEGER 
 SPECIFIC ngramm_u_pos 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 RETURNS NULL ON NULL INPUT 
 EXTERNAL NAME 'CS!ngramm_u_pos!D:\Documents and Settings\chongmi\Desktop\UDF\ngramm_u_pos.c!'

--
/* <sc-function> TUDF.reformatprofile_u </sc-function> */
REPLACE FUNCTION TUDF.REFORMATPROFILE_U 
  (inputprofile VARCHAR(127) CHARACTER SET UNICODE) 
 RETURNS INTEGER 
 SPECIFIC reformatprofile_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!reformatprofile_u!D:\Documents and Settings\chongmi\Desktop\UDF\rowsec\reformatprofile_u.c'

--
/* <sc-function> TUDF.testRSACp_u </sc-function> */
REPLACE FUNCTION TUDF.TESTRSACP_U 
  (RSAC INTEGER, 
   intprofile INTEGER) 
 RETURNS CHAR(1) CHARACTER SET LATIN 
 SPECIFIC testRSACp_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!testRSACp_u!D:\Documents and Settings\chongmi\Desktop\UDF\rowsec\testRSACp_u.c'

--
/* <sc-function> TUDF.testRSAC_u </sc-function> */
REPLACE FUNCTION TUDF.TESTRSAC_U 
  (RSAC INTEGER, 
   intprofile INTEGER) 
 RETURNS CHAR(1) CHARACTER SET LATIN 
 SPECIFIC testRSAC_u 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!testRSAC_u!D:\Documents and Settings\chongmi\Desktop\UDF\rowsec\testRSAC_u.c'

--
/* <sc-function> UDGPAUD.hash_md5 </sc-function> */
REPLACE FUNCTION UDGPAUD.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> udmahvc.TimeStamp_Diff_Seconds </sc-function> */
REPLACE FUNCTION udmahvc.TimeStamp_Diff_Seconds
(
   ts1 TIMESTAMP(6)
  ,ts2 TIMESTAMP(6)
)
RETURNS DECIMAL(18,6)
LANGUAGE SQL
CONTAINS SQL
DETERMINISTIC
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN 
(Cast((Cast(ts2 AS DATE AT 0)- Cast(ts1 AS DATE AT 0)) AS DECIMAL(18,6)) * 86400)
      + ((Extract(  HOUR From ts2) - Extract(  HOUR From ts1)) * 3600)
      + ((Extract(MINUTE From ts2) - Extract(MINUTE From ts1)) * 60)
      +  (Extract(SECOND From ts2) - Extract(SECOND From ts1));

--
/* <sc-function> UDPARM.pnorm </sc-function> */
create function udparm.pnorm ( a float)
returns float
language sql
deterministic
CONTAINS SQL
SPECIFIC udparm.pnorm
CALLED ON NULL INPUT
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN CASE 
  WHEN a >= 0 
  THEN 1 - POWER(((((((0.000005383*a+0.0000488906)*a+0.0000380036)*a+0.0032776263)*a+0.0211410061)*a+0.049867347)*a+1),-16)/2
  ELSE 1 - (1 - POWER(((((((0.000005383*(-1)*a+0.0000488906)*a*(-1)+0.0000380036)*a*(-1)+0.0032776263)*a*(-1)+0.0211410061)*a*(-1)+0.049867347)*a*(-1)+1),-16)/2)
	END;

--
/* <sc-function> UDPARM.pow </sc-function> */
create function udparm.pow ( a float)
returns float
language sql
deterministic
CONTAINS SQL
SPECIFIC udparm.pow
CALLED ON NULL INPUT
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN CASE 
  WHEN a > 1 
  THEN POWER(a, 3)
  ELSE -1
	END;

--
/* <sc-function> UDPARM.qnorm </sc-function> */
create function udparm.qnorm ( a float)
returns float
language sql
deterministic
CONTAINS SQL
SPECIFIC udparm.qnorm
CALLED ON NULL INPUT
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN CASE 
 when a = 0
 then Null
 when a = 1
 then Null
  WHEN a < 0.02425
  THEN (((((-7.784894002430293*0.001*sqrt(-2*ln(a))+-3.223964580411365*0.1)*sqrt(-2*ln(a))+-2.400758277161838)*sqrt(-2*ln(a))+-2.549732539343734)*sqrt(-2*ln(a))+4.374664141464968)*sqrt(-2*ln(a))+2.938163982698783)/
  ((((7.784695709041462*0.001*sqrt(-2*ln(a))+3.224671290700398*0.1)*sqrt(-2*ln(a))+2.445134137142996)*sqrt(-2*ln(a))+3.754408661907416)*sqrt(-2*ln(a))+1)
  WHEN
  a >= 0.02425 and a <= 1-0.02425
  THEN
  (a-0.5) * (((((-3.969683028665376*10*power(a-0.5,2)+2.209460984245205*100)*power(a-0.5,2)+-2.759285104469687*100)*power(a-0.5,2)+1.383577518672690*100)*power(a-0.5,2)+-3.066479806614716*10)*power(a-0.5,2)+2.506628277459239)/
  (((((-5.447609879822406*10*power(a-0.5,2)+1.615858368580409*100)*power(a-0.5,2)+-1.556989798598866*100)*power(a-0.5,2)+6.680131188771972*10)*power(a-0.5,2)+-1.328068155288572*10)*power(a-0.5,2)+1)
  ELSE
  -(((((-7.784894002430293*0.001*sqrt(-2*ln(1-a))+-3.223964580411365*0.1)*sqrt(-2*ln(1-a))+-2.400758277161838)*sqrt(-2*ln(1-a))+-2.549732539343734)*sqrt(-2*ln(1-a))+4.374664141464968)*sqrt(-2*ln(1-a))+2.938163982698783)/
  ((((7.784695709041462*0.001*sqrt(-2*ln(1-a))+3.224671290700398*0.1)*sqrt(-2*ln(1-a))+2.445134137142996)*sqrt(-2*ln(1-a))+3.754408661907416)*sqrt(-2*ln(1-a))+1)
	END;

--
/* <sc-function> UDSASOPS.S1Test_scorestub </sc-function> */
REPLACE FUNCTION UDSASOPS.S1TEST_SCORESTUB () 
 RETURNS INTEGER 
 SPECIFIC S1Test_scorestub 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'SL!"jazxfbrs"!NS!CI!jazz!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/jazz.h!CI!ufmt!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/ufmt.h!CI!ds2w!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/ds2w.h!CI!jazznlsp!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/jazznlsp.h!CI!jazzencodings!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/jazzencodings.h!CI!jazzlocales!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/jazzlocales.h!CI!tkcsparm!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/tkcsparm.h!CS!S1Test_S1Test!W:/SASWORK/temp/_TD19292_AAUNSWZ15_/Prc2/tempdir/S1Test.c'

--
/* <sc-function> UFTDWRK.ChkInteger_New </sc-function> */
replace function UFTDWRK.ChkInteger_New 
(
   i_String  varchar(255) character set latin 
) 
  returns integer
  specific ChkInteger_New 
  returns null on null input
  deterministic
  contains sql
  collation invoker
  inline type 1
  return (
           -- This is the NO CODE change to "DT" views version
           -- It MUST behave in EXACTLY THE SAME WAY the the external C function it replaces
           -- If the i_string is not null (most often the case) then
           --      If it is an invalid integer then return 0 
           --      otherwise return 1 
           -- elsif i_String is null then 
           --     return null
           -- end
           case when i_String is not null then
                case when trycast(i_string as integer) is null then 0 
                    else 1
                end
           else
               null
           end
);

--
/* <sc-function> UFTDWRK.ChkInteger_New_Test </sc-function> */
replace function UFTDWRK.ChkInteger_New_Test 
(
   i_String  varchar(255) character set latin 
) 
  returns integer
  specific ChkInteger_New_Test 
  returns null on null input
  --deterministic
  contains sql
  collation invoker
  inline type 1
  return (
           -- This is the NO CODE change to "DT" views version
           -- It MUST behave in EXACTLY THE SAME WAY the the external C function it replaces
           -- If the i_string is not null (most often the case) then
           --      If it is an invalid integer then return 0 
           --      otherwise return 1 
           -- elsif i_String is null then 
           --     return null
           -- end
           case when i_String is not null then 
                case when trycast(i_string as integer) is null then 0 
                    else 1
                end
           --else
               --null
           end
);

--
/* <sc-function> UMBREPROF.QualNewAcct </sc-function> */
REPLACE FUNCTION UMBREPROF.QUALNEWACCT 
  (prodCode VARCHAR(40) CHARACTER SET LATIN, 
   transAmt VARCHAR(40) CHARACTER SET LATIN, 
   transCnt VARCHAR(40) CHARACTER SET LATIN) 
 RETURNS VARCHAR(250) CHARACTER SET LATIN 
 SPECIFIC QualNewAcct 
 LANGUAGE CPP 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CI!call_ws!call_ws.h!CI!soapStub!soapStub.h!CI!soapH!soapH.h!CI!soapServicesBindingProxy!soapServicesBindingProxy.h!CI!ServicesBinding!ServicesBinding.h!CI!stdsoap2!stdsoap2.h!CS!qnacct!qnacct.cpp!CS!call_ws!call_ws.cpp!CS!soapC!soapC.cpp!CS!proxy!soapServicesBindingProxy.cpp!CS!stdsoap2!stdsoap2.cpp!F!udfQualityNewAcct'

--
/* <sc-function> U_D_DSV_001_AAG_1.hash_md5 </sc-function> */
REPLACE FUNCTION U_D_DSV_001_AAG_1.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> U_D_DSV_001_CSA_1.calc_tnpe_crr </sc-function> */
create function u_d_dsv_001_csa_1.calc_tnpe_crr(pd_grade varchar(2), lgd_grade varchar(1))
returns byteint contains sql language sql deterministic collation invoker inline type 1
return (
  case
    when pd_grade in ('F','F1','F2','F3') and lgd_grade in ('D','E','F','N','G','M') then 7
    when pd_grade in ('G','G1','G2','G3') and lgd_grade in ('A','B','C') then 7
    when pd_grade in ('G','G1','G2','G3') and lgd_grade in ('D','E','F','N','G','M') then 8
    when pd_grade in ('H') and lgd_grade in ('A','B','C') then 8
    when pd_grade in ('H','G') then 9
    else 6
  end
);

--
/* <sc-function> U_D_DSV_001_CSA_1.flag_tnpa </sc-function> */
replace function u_d_dsv_001_csa_1.flag_tnpa(pd_grade varchar(1), lgd_grade varchar(1))
returns byteint contains sql language sql deterministic collation invoker inline type 1
return (
  case
    when lower(pd_grade) = 'f' and lower(lgd_grade) not in ('a', 'b', 'c') then 1
    when lower(pd_grade) in ('g', 'h', 'r') then 1
    else 0
  end
)

--
/* <sc-function> U_D_DSV_001_CSA_1.prdf_grde_rank </sc-function> */
create function u_d_dsv_001_csa_1.prdf_grde_rank(pd_grade varchar(2))
returns byteint contains sql language sql deterministic collation invoker inline type 1
return (
  case
    when length(lower(pd_grade)) = 2 then
      (ascii(lower(substr(pd_grade, 1, 1))) - 97) * 3 +
        cast(substr(pd_grade, 2, 1) as integer)
  end
);

--
/* <sc-function> U_D_DSV_001_RAR_0.CALC_AGE </sc-function> */
--Function for calculating age at a given date from date of birth
REPLACE FUNCTION U_D_DSV_001_RAR_0.CALC_AGE(
  BIRTH_DATE DATE,
  AGE_AT_DATE DATE
)
RETURNS INTEGER
CONTAINS SQL LANGUAGE SQL
DETERMINISTIC
COLLATION INVOKER
INLINE TYPE 1
RETURN (
  YEAR(AGE_AT_DATE) - YEAR(BIRTH_DATE) + CASE
    WHEN MONTH(BIRTH_DATE) > MONTH(AGE_AT_DATE) THEN -1
    WHEN MONTH(BIRTH_DATE) = MONTH(AGE_AT_DATE) AND EXTRACT(DAY FROM BIRTH_DATE) > EXTRACT(DAY FROM AGE_AT_DATE) THEN -1
    ELSE 0
  END
)

--
/* <sc-function> U_D_DSV_001_RAR_0.hash_md5 </sc-function> */
REPLACE FUNCTION U_D_DSV_001_RAR_0.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> U_D_DSV_001_RAR_0.PMT </sc-function> */
REPLACE FUNCTION U_D_DSV_001_RAR_0.PMT(
	interest_rate DECIMAL (15, 10),
    num_repayments INTEGER,
	principal DECIMAL(18, 2)
)
RETURNS DECIMAL(18, 2)
CONTAINS SQL LANGUAGE SQL
DETERMINISTIC
COLLATION INVOKER
INLINE TYPE 1
RETURN
(-principal * interest_rate) / NULLIFZERO(1 - POWER((1 + interest_rate), (-1 * num_repayments)));

--
/* <sc-function> U_D_DSV_001_RGQ_1.select_table_column </sc-function> */

-- Example 1
-- 1: how to create the function
CREATE FUNCTION U_D_DSV_001_RGQ_1.select_table_column ( -- where to store the function and the functions name; must have full database access to save the function to
    table_name VARCHAR(255),                            -- argument and type
    column_name VARCHAR(255)                            -- argument and type
) RETURNS VARCHAR(255)                                  -- return object and type
CONTAINS SQL                                            -- specifies there is SQL code within the function
DETERMINISTIC                                           -- want the same result every single time
RETURNS NULL ON NULL INPUT                              -- what we expect if nothing is passed in to the function arguments
SQL SECURITY DEFINER                                    -- no idea
COLLATION INVOKER                                       -- required for string
INLINE TYPE 1                                           -- no idea why this is needed
RETURN table_name || '.' || column_name;

--
/* <sc-function> U_P_DSV_001_ORA_1.FN_DataTypeString </sc-function> */

  REPLACE FUNCTION u_p_dsv_001_ora_1.FN_DataTypeString
 (
  ColumnType CHAR(2),
  ColumnLength INT,
  DecimalTotalDigits SMALLINT,
  DecimalFractionalDigits SMALLINT,
  CharType SMALLINT,
  ColumnUDTName VARCHAR(128) CHARACTER SET UNICODE
 )
RETURNS VARCHAR(60)
LANGUAGE SQL
CONTAINS SQL
DETERMINISTIC
SQL SECURITY DEFINER
COLLATION INVOKER
INLINE TYPE 1
RETURN 
  CASE ColumnType
    WHEN 'BF' THEN 'BYTE('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
    WHEN 'BV' THEN 'VARBYTE('         || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
    WHEN 'CF' THEN 'CHAR('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
    WHEN 'CV' THEN 'VARCHAR('         || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
    WHEN 'D ' THEN 'DECIMAL('         || TRIM(DecimalTotalDigits) || ','
                                      || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'DA' THEN 'DATE'
    WHEN 'F ' THEN 'FLOAT'
    WHEN 'I1' THEN 'BYTEINT'
    WHEN 'I2' THEN 'SMALLINT'
    WHEN 'I8' THEN 'BIGINT'
    WHEN 'I ' THEN 'INTEGER'
    WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
    WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
    WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits) || ')'
    WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO MONTH'
    WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits) || ')'
    WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'
    WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO HOUR'
    WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO MINUTE'
    WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
                                      || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'
    WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO MINUTE'
    WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
                                      || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits) || ')'
    WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
                                      || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits) || ',' 
                                      || TRIM(DecimalFractionalDigits) || ')'
    WHEN 'BO' THEN 'BLOB('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
    WHEN 'CO' THEN 'CLOB('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'

    WHEN 'PD' THEN 'PERIOD(DATE)'     
    WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
    WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits) || '))'
    WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits) || '))'
    WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits) || '))' || ' WITH TIME ZONE'
    WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)

    WHEN '++' THEN 'TD_ANYTYPE'
    WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits) END
                                      || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits) END
                                      || ')'
    WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
    WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)

    ELSE '<Unknown> ' || ColumnType
  END 
  || CASE
        WHEN ColumnType IN ('CV', 'CF', 'CO') 
        THEN CASE CharType 
                WHEN 1 THEN ' CHARACTER SET LATIN'
                WHEN 2 THEN ' CHARACTER SET UNICODE'
                WHEN 3 THEN ' CHARACTER SET KANJISJIS'
                WHEN 4 THEN ' CHARACTER SET GRAPHIC'
                WHEN 5 THEN ' CHARACTER SET KANJI1'
                ELSE ''
             END
         ELSE ''
      END;

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2bigint </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2BIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_bigint BIGINT) 
 RETURNS BIGINT 
 SPECIFIC char2bigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2bigint!./char2bigint.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2byteint </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2BYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_byteint BYTEINT) 
 RETURNS BYTEINT 
 SPECIFIC char2byteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2byteint!./char2byteint.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2date </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2DATE 
  (input_date_string VARCHAR(255) CHARACTER SET LATIN, 
   default_date DATE) 
 RETURNS DATE 
 SPECIFIC char2date 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getadate!./getadate.c!CS!char2date!./char2date.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2decimal1 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2DECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(2,0)) 
 RETURNS DECIMAL(2,0) 
 SPECIFIC char2decimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal1!./char2decimal1.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2decimal2 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2DECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(4,0)) 
 RETURNS DECIMAL(4,0) 
 SPECIFIC char2decimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal2!./char2decimal2.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2decimal4 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2DECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(9,0)) 
 RETURNS DECIMAL(9,0) 
 SPECIFIC char2decimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal4!./char2decimal4.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2decimal8 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2DECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER, 
   default_decimal DECIMAL(18,0)) 
 RETURNS DECIMAL(18,0) 
 SPECIFIC char2decimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2decimal8!./char2decimal8.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2integer </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2INTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_integer INTEGER) 
 RETURNS INTEGER 
 SPECIFIC char2integer 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2integer!./char2integer.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2smallint </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2SMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   default_smallint SMALLINT) 
 RETURNS SMALLINT 
 SPECIFIC char2smallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2smallint!./char2smallint.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2time </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2TIME 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_time TIME(6)) 
 RETURNS TIME(6) 
 SPECIFIC char2time 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!getatime!./getatime.c!CS!char2time!./char2time.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.char2timestamp </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHAR2TIMESTAMP 
  (input_time_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   default_timestamp TIMESTAMP(6)) 
 RETURNS TIMESTAMP(6) 
 SPECIFIC char2timestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!char2timestamp!./char2timestamp.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkbigint </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKBIGINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BIGINT 
 SPECIFIC chkbigint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkbigint!./chkbigint.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkbyteint </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKBYTEINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkbyteint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chknum!./chknum.c!CS!chkbyteint!./chkbyteint.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkdate </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKDATE 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS BYTEINT 
 SPECIFIC chkdate 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdate!./chkdate.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkdecimal1 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKDECIMAL1 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal1 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdec!./chkdec.c!CS!chkdecimal1!./chkdecimal1.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkdecimal16 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKDECIMAL16 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal16 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal16!./chkdecimal16.c!CS!chkdec16!./chkdec16.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkdecimal2 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKDECIMAL2 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal2 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal2!./chkdecimal2.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkdecimal4 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKDECIMAL4 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal4 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal4!./chkdecimal4.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkdecimal8 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKDECIMAL8 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER, 
   expected_scale INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chkdecimal8 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkdecimal8!./chkdecimal8.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkfloat </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKFLOAT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkfloat 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkfl!./chkfl.c!CS!chkfloat!./chkfloat.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chkinteger </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKINTEGER 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC chkinteger 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chkinteger!./chkinteger.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chksmallint </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKSMALLINT 
  (input_string VARCHAR(255) CHARACTER SET LATIN) 
 RETURNS SMALLINT 
 SPECIFIC chksmallint 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chksmallint!./chksmallint.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chktime </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKTIME 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktime 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktime!./chktime.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.chktimestamp </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.CHKTIMESTAMP 
  (input_string VARCHAR(255) CHARACTER SET LATIN, 
   expected_precision INTEGER) 
 RETURNS BYTEINT 
 SPECIFIC chktimestamp 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!chktimestamp!./chktimestamp.c'

--
/* <sc-function> V_O00_F_TCF_001_STD_0.hash_md5 </sc-function> */
REPLACE FUNCTION V_O00_F_TCF_001_STD_0.HASH_MD5 
  (arg VARCHAR(32000) CHARACTER SET LATIN) 
 RETURNS CHAR(32) CHARACTER SET LATIN 
 SPECIFIC hash_md5 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE TD_GENERAL 
 NOT DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'ci:md5:md5.h:cs:md5:md5.c:cs:md5_latin:udf_md5_latin.c:F:md5_latin'

--
/* <sc-function> V_O01_F_TDS_001_STD_0.GetReqId </sc-function> */
REPLACE FUNCTION V_O01_F_TDS_001_STD_0.GetReqId
(
    i_Prefix VARCHAR(6)
)
  RETURNS VARCHAR(29)
  SPECIFIC V_O01_F_TDS_001_STD_0.GetReqId
  RETURNS NULL ON NULL INPUT
  DETERMINISTIC
  CONTAINS SQL
  COLLATION INVOKER
  INLINE TYPE 1
  RETURN (
            -- Prefix
            CAST(LPAD(TRIM(i_Prefix),6,'_') AS CHAR(6)) -- Prefix
         || '_'
            -- Session Number
         || TRIM(CAST( (CAST(SESSION AS INTEGER) (FORMAT '9999999999')) AS CHAR(10))) -- Session
         || '_'
            -- Timestamp
         || CAST(LPAD(
                TRIM((CAST( CURRENT_TIMESTAMP(0) AS DATE) - DATE '2000-01-01') * (86400 (BIGINT))
                           + (EXTRACT(HOUR   FROM CURRENT_TIMESTAMP(0) ) * (3600 (BIGINT)))
                           + (EXTRACT(MINUTE FROM CURRENT_TIMESTAMP(0) ) * (60 (BIGINT)))
                           + (EXTRACT(SECOND FROM CURRENT_TIMESTAMP(0) ))
                           -- Extra Precision
                           + SUBSTR(CAST(CURRENT_TIMESTAMP(2) AS TIMESTAMP(2) )(FORMAT 'HH:MI:SS.S(2)') (char(11)),10,2)
              )
              ,11,'0') AS CHAR(11))
);

--
/* <sc-function> V_O01_F_TDS_001_STD_0.TDS_Put_Trace_Msg </sc-function> */
REPLACE FUNCTION V_O01_F_TDS_001_STD_0.TDS_PUT_TRACE_MSG 
  (trace_type VARCHAR(30) CHARACTER SET LATIN, 
   trace_ts TIMESTAMP(6), 
   num_01 DECIMAL(38,18), 
   num_02 DECIMAL(38,18), 
   num_03 DECIMAL(38,18), 
   num_04 DECIMAL(38,18), 
   num_05 DECIMAL(38,18), 
   num_06 DECIMAL(38,18), 
   num_07 DECIMAL(38,18), 
   num_08 DECIMAL(38,18), 
   num_09 DECIMAL(38,18), 
   num_10 DECIMAL(38,18), 
   date_01 DATE, 
   date_02 DATE, 
   date_03 DATE, 
   date_04 DATE, 
   date_05 DATE, 
   ts_01 TIMESTAMP(6), 
   ts_02 TIMESTAMP(6), 
   ts_03 TIMESTAMP(6), 
   ts_04 TIMESTAMP(6), 
   ts_05 TIMESTAMP(6), 
   varchar_s_01 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_02 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_03 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_04 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_05 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_06 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_07 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_08 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_09 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_s_10 VARCHAR(255) CHARACTER SET LATIN, 
   varchar_l_01 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_02 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_03 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_04 VARCHAR(4095) CHARACTER SET LATIN, 
   varchar_l_05 VARCHAR(4095) CHARACTER SET LATIN) 
 RETURNS INTEGER 
 SPECIFIC TDS_Put_Trace_Msg 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS!TDS_Put_Trace_Msg!TDS_Put_Trace_Msg.c!F!TDS_Put_Trace_Msg'

--
/* <sc-function> V_O01_F_TDS_001_STD_0.XOR4BYTE </sc-function> */
REPLACE FUNCTION V_O01_F_TDS_001_STD_0.XOR4BYTE 
  (P1 BYTE(4)) 
 RETURNS CHAR(8) CHARACTER SET LATIN 
 CLASS AGGREGATE (60)
 SPECIFIC XOR4BYTE 
 LANGUAGE C 
 NO SQL
 NO EXTERNAL DATA
 PARAMETER STYLE SQL 
 DETERMINISTIC 
 CALLED ON NULL INPUT 
 EXTERNAL NAME 'CS;XOR4BYTE;XOR4BYTE.c'

