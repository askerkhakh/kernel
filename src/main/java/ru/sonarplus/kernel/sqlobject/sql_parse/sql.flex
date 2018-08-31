   
/* --------------------------Usercode Section------------------------ */
package ru.sonarplus.kernel.sqlobject.sql_parse;
   
import java_cup.runtime.*;

%%
   
/* -----------------Options and Declarations Section----------------- */
   
/* 
   The name of the class JFlex will create will be Lexer.
   Will write the code to the file Lexer.java. 
*/
%class SqlLexer

%public

/*
  The current line number can be accessed with the variable yyline
  and the current column number with the variable yycolumn.
*/
%line
%column
    
/* 
   Will switch to a CUP compatibility mode to interface with a CUP
   generated parser.
*/
%cup
%ignorecase
   
/*
  Declarations
   
  Code between %{ and %}, both of which must be at the beginning of a
  line, will be copied letter to letter into the lexer class source.
  Here you declare member variables and functions that are used inside
  scanner actions.  
*/
%{   
    /* To create a new java_cup.runtime.Symbol with information about
       the current token, the token will have no value in this
       case. */
    private Symbol symbol(int type) {
        return new Symbol(type, yyline, yycolumn);
    }
    
    /* Also creates a new java_cup.runtime.Symbol with information
       about the current token, but this object has a value. */
    private Symbol symbol(int type, Object value) {
        return new Symbol(type, yyline, yycolumn, value);
    }
%}
   

/*
  Macro Declarations
  
  These declarations are regular expressions that will be used latter
  in the Lexical Rules Section.  
*/
   
/* A line terminator is a \r (carriage return), \n (line feed), or
   \r\n. */
LineTerminator = \r|\n|\r\n
   
/* White space is a line terminator, space, tab, or line feed. */
WhiteSpace     = {LineTerminator} | [ \t\f]
   
/* A literal integer is is a number beginning with a number between
   one and nine followed by zero or more numbers between zero and nine
   or just a zero.  */
dec_int_lit = 0 | [1-9][0-9]*
   
/* A identifier integer is a word beginning a letter between A and
   Z, a and z, or an underscore followed by zero or more letters
   between A and Z, a and z, zero and nine, or an underscore. */
dec_int_id = [A-Za-z_][A-Za-z_0-9]*

string_lit = (\'(\\.|[^\'])*\')+
escape_sequence=\\[^\r\n]
ident_lit = [_a-zA-Z\u0401\u0451\u0410-\u044F\:][_a-zA-Z\u0401\u0451\u0410-\u044F0-9\_\$\#]*|(\"(\\.|[^\"])*\")+
//operator_lit =  [\=\<\>\!]+
plusminus_lit =  [\+\-]
number_lit = ([0-9]|\.[0-9]|\$[0-9]|0x)[0-9\.abcdef]*(e[\+\-]?[0-9]*)?[fd]?
groupby_lit = GROUP[ \t\n]+BY
orderby_lit = ORDER[ \t\n]+BY
partitionby_lit = PARTITION[ \t\n]+BY
concat_lit = \|\|
comment_lit = (\/\*([^])*?\*\/)|(--.*)
join_operator =  (NATURAL[ \t\n]+)?(FULL[ \t\n]+|LEFT[ \t\n]+|RIGHT[ \t\n]+)?(OUTER[ \t\n]+|INNER[ \t\n]+)?JOIN
cross_join_operator = CROSS[\ \t\n]+JOIN
%%
/* ------------------------Lexical Rules Section---------------------- */
   
/*
   This section contains regular expressions and actions, i.e. Java
   code, that will be executed when the scanner matches the associated
   regular expression. */
   
   /* YYINITIAL is the state at which the lexer begins scanning.  So
   these regular expressions will only be matched if the scanner is in
   the start state YYINITIAL. */
   
/* keywords */
<YYINITIAL> "as" { return symbol(SqlParserSym.AS); }
<YYINITIAL> "select" { return symbol(SqlParserSym.SELECT); }
<YYINITIAL> "from" { return symbol(SqlParserSym.FROM); }
<YYINITIAL> "where" { return symbol(SqlParserSym.WHERE); }
<YYINITIAL> "and" { return symbol(SqlParserSym.AND); }
<YYINITIAL> "or" { return symbol(SqlParserSym.OR); }
<YYINITIAL> "in" { return symbol(SqlParserSym.IN); }
<YYINITIAL> "case" { return symbol(SqlParserSym.CASE); }
<YYINITIAL> "when" { return symbol(SqlParserSym.WHEN); }
<YYINITIAL> "then" { return symbol(SqlParserSym.THEN); }
<YYINITIAL> "else" { return symbol(SqlParserSym.ELSE); }
<YYINITIAL> "end" { return symbol(SqlParserSym.END); }
<YYINITIAL> "subord_or_equal" { return symbol(SqlParserSym.SUBORD_OR_EQUAL); }
<YYINITIAL> "subord" { return symbol(SqlParserSym.SUBORD); }
<YYINITIAL> "subord_direct" { return symbol(SqlParserSym.SUBORD_DIRECT); }
<YYINITIAL> "subord_direct_or_equal" { return symbol(SqlParserSym.SUBORD_DIRECT_OR_EQUAL); }
<YYINITIAL> "leaf" { return symbol(SqlParserSym.LEAF); }
<YYINITIAL> "leaf_or_equal" { return symbol(SqlParserSym.LEAF_OR_EQUAL); }
<YYINITIAL> "to_code" { return symbol(SqlParserSym.TO_CODE); }
<YYINITIAL> "exists" { return symbol(SqlParserSym.EXISTS); }
<YYINITIAL> "between" { return symbol(SqlParserSym.BETWEEN); }
<YYINITIAL> "scalar" { return symbol(SqlParserSym.SCALAR); }
<YYINITIAL> "over" { return symbol(SqlParserSym.OVER); }
<YYINITIAL> "like" { return symbol(SqlParserSym.LIKE); }
<YYINITIAL> "true" { return symbol(SqlParserSym.TRUE); }
<YYINITIAL> "false" { return symbol(SqlParserSym.FALSE); }
<YYINITIAL> "date" { return symbol(SqlParserSym.DATE); }
<YYINITIAL> "time" { return symbol(SqlParserSym.TIME); }
<YYINITIAL> "timestamp" { return symbol(SqlParserSym.TIMESTAMP); }
<YYINITIAL> "is" { return symbol(SqlParserSym.IS); }
<YYINITIAL> "with" { return symbol(SqlParserSym.WITH); }
<YYINITIAL> "cycle" { return symbol(SqlParserSym.CYCLE); }
<YYINITIAL> "to" { return symbol(SqlParserSym.TO); }
<YYINITIAL> "default" { return symbol(SqlParserSym.DEFAULT); }
<YYINITIAL> "on" { return symbol(SqlParserSym.ON); }
<YYINITIAL> "not" { return symbol(SqlParserSym.NOT); }
<YYINITIAL> "asc" { return symbol(SqlParserSym.ASC); }
<YYINITIAL> "desc" { return symbol(SqlParserSym.DESC); }
<YYINITIAL> "having" { return symbol(SqlParserSym.HAVING); }
<YYINITIAL> "distinct" { return symbol(SqlParserSym.DISTINCT); }
<YYINITIAL> "null" { return symbol(SqlParserSym.NULL); }
<YYINITIAL> "extract" { return symbol(SqlParserSym.EXTRACT); }
<YYINITIAL> "year" { return symbol(SqlParserSym.YEAR); }
<YYINITIAL> "month" { return symbol(SqlParserSym.MONTH); }
<YYINITIAL> "day" { return symbol(SqlParserSym.DAY); }
<YYINITIAL> "hour" { return symbol(SqlParserSym.HOUR); }
<YYINITIAL> "minute" { return symbol(SqlParserSym.MINUTE); }
<YYINITIAL> "second" { return symbol(SqlParserSym.SECOND); }
<YYINITIAL> "timezone_hour" { return symbol(SqlParserSym.TIMEZONE_HOUR); }
<YYINITIAL> "timezone_minute" { return symbol(SqlParserSym.TIMEZONE_MINUTE); }
<YYINITIAL> "delete" { return symbol(SqlParserSym.DELETE); }
<YYINITIAL> "insert" { return symbol(SqlParserSym.INSERT); }
<YYINITIAL> "into" { return symbol(SqlParserSym.INTO); }
<YYINITIAL> "values" { return symbol(SqlParserSym.VALUES); }
<YYINITIAL> "delete" { return symbol(SqlParserSym.DELETE); }
<YYINITIAL> "update" { return symbol(SqlParserSym.UPDATE); }
<YYINITIAL> "set" { return symbol(SqlParserSym.SET); }
<YYINITIAL> "call" { return symbol(SqlParserSym.CALL); }
<YYINITIAL> "escape" { return symbol(SqlParserSym.ESCAPE); }
<YYINITIAL> "union" { return symbol(SqlParserSym.UNION); }
<YYINITIAL> "all" { return symbol(SqlParserSym.ALL); }
<YYINITIAL> "except" { return symbol(SqlParserSym.EXCEPT); }
<YYINITIAL> "minus" { return symbol(SqlParserSym.MINUS); }
<YYINITIAL> "intersect" { return symbol(SqlParserSym.INTERSECT); }
<YYINITIAL> "regexp_match" { return symbol(SqlParserSym.REGEXP_MATCH); }
<YYINITIAL> "offset" {return symbol(SqlParserSym.OFFSET); }
<YYINITIAL> "fetch" {return symbol(SqlParserSym.FETCH); }
<YYINITIAL> "first" {return symbol(SqlParserSym.FIRST); }
<YYINITIAL> "rows" {return symbol(SqlParserSym.ROWS); }
<YYINITIAL> "only" {return symbol(SqlParserSym.ONLY); }


<YYINITIAL> {
    ","                   { return symbol(SqlParserSym.COMMA); }
    "*"                   { return symbol(SqlParserSym.STAR); }
    "."                   { return symbol(SqlParserSym.DOT); }
    "/"                   { return symbol(SqlParserSym.DIV); }
    "("                   { return symbol(SqlParserSym.PARENT_L); }
    ")"                   { return symbol(SqlParserSym.PARENT_R); }
    {string_lit}          {
        return symbol(SqlParserSym.STRING, SqlParseSupport.dequoteLiteralString(yytext()));
    }
    {ident_lit}           {
        return symbol(SqlParserSym.IDENTIFIER, SqlParseSupport.dequoteIdentifier(yytext()));
    }

    // приходится делать так, в противном случае знак присвоения '=' в запросе UPDATE неверно при разборе интерпретируется
    "="                   { return symbol(SqlParserSym.EQUAL, yytext()); }
    "<>"                   { return symbol(SqlParserSym.NOT_EQUAL, yytext()); }
    "<"                   { return symbol(SqlParserSym.LESS, yytext()); }
    "<="                   { return symbol(SqlParserSym.LESS_EQUAL, yytext()); }
    ">"                   { return symbol(SqlParserSym.GREAT, yytext()); }
    ">="                   { return symbol(SqlParserSym.GREAT_EQUAL, yytext()); }
    //{operator_lit}        { return symbol(SqlParserSym.OPERATOR, yytext()); }

    {plusminus_lit}       { return symbol(SqlParserSym.PLUS_MINUS, yytext()); }
    {number_lit}          { return symbol(SqlParserSym.NUMBER, yytext()); }
    {groupby_lit}         { return symbol(SqlParserSym.GROUP_BY); }
    {orderby_lit}         { return symbol(SqlParserSym.ORDER_BY); }
    {join_operator}       { return symbol(SqlParserSym.JOIN_OPERATOR, yytext());}
    {cross_join_operator} { return symbol(SqlParserSym.CROSS_JOIN_OPERATOR, yytext());}
    {concat_lit}          { return symbol(SqlParserSym.CONCAT);}
    {partitionby_lit}     { return symbol(SqlParserSym.PARTITION_BY);}
    {comment_lit}         { if (yytext().startsWith("--+") || yytext().startsWith("/*+")) { return symbol(SqlParserSym.COMMENT, yytext());} }
    {WhiteSpace} { /* ignore */ }
    "`"                   { return symbol(SqlParserSym.BACK_QUOTE); }
   
}


/* No token was found for the input so through an error.  Print out an
   Illegal character message with the illegal character that was found. */
[^]                    { throw new Error("Неверный символ <"+yytext()+">"); }
