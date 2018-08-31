package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders;

public class JsonSqlObjectBuilderConsts {

    public static final String KIND_BETWEEN = "between";
    public static final String KIND_CASE_SEARCH = "case_search";
    public static final String KIND_CASE_SIMPLE = "case_simple";
    public static final String KIND_CODE_COMPARISON = "code_cmp";
    public static final String KIND_COMPARISON = "cmp";
    public static final String KIND_CONDITIONS = "conditions";
    public static final String KIND_CTE_CYCLE = "cte_cycle";
    public static final String KIND_CURSOR_SPEC = "cursor_spec";
    public static final String KIND_DELETE = "delete";
    public static final String KIND_EXISTS = "exists";
    public static final String KIND_EXPRESSION = "expr";
    public static final String KIND_FIELDNAME = "name";
    public static final String KIND_FIELDRNAME = "rname";
    public static final String KIND_FULLTEXTSEARCH = "fts";
    public static final String KIND_FULLTEXTSEARCH_MARKER = "fts_marker";
    public static final String KIND_GROUPBY = "groupby";
    public static final String KIND_IN_SELECT = "in_select";
    public static final String KIND_IN_TUPLE = "in_tuple";
    public static final String KIND_INSERT= "insert";
    public static final String KIND_ISNULL = "isnull";
    public static final String KIND_JOIN_LEFT = "left";
    public static final String KIND_JOIN_RIGHT = "right";
    public static final String KIND_JOIN_INNER = "inner";
    public static final String KIND_JOIN_FULL_OUTER = "fullouter";
    public static final String KIND_LIKE = "like";
    public static final String KIND_PARAMETER = "param";
    public static final String KIND_PARAMSTATIC = "param_static";
    public static final String KIND_PARAMS = "params_clause";
    public static final String KIND_RECID = "recid";
    public static final String KIND_REGEXP_LIKE = "regexp_like";
    public static final String KIND_SP = "callsp";
    public static final String KIND_UPDATE = "update";
    public static final String KIND_VALUE_RECID = "value_recid";

    /* #BAD# Как-то так получилось, что для объектов TSelect и TSource_Query
       совпали kind'ы (stKindSelect и stKindSourceSelect), используемые при их сериализации.
       TODO устранить недочёт и убедиться, что конвертация json в sql происходит по-прежнему корректно (а как она до этого работала?).
     */
    public static final String KIND_SELECT = "select";
    public static final String KIND_SOURCE_SELECT = "source_select";

    public static final String KIND_SOURCE_TABLE = "table";
    public static final String KIND_UNION_ITEM = "union_item";
    public static final String KIND_VALUE = "value";

    public static final String KIND_TRS_SET_SAVEPOINT = "set_savepoint";
    public static final String KIND_TRS_COMMIT = "commit";
    public static final String KIND_TRS_ROLLBACK = "rollback";
    public static final String KIND_TRS_REMOVE_SAVEPOINT = "remove_savepoint";
    public static final String KIND_TRS_ROLLBACK_TO = "rollback_to";
    public static final String KIND_TRS_SET_TRANSACTION = "set_transaction";

    public static final String KEY_TRS_STMT = "stmt";
    public static final String KEY_TRS_SAVEPOINT_NAME = "name";

    public static final String KEY_ALIAS = "alias";
    public static final String KEY_ARGS = "args";
    public static final String KEY_CASE_EXPR = "case_expr";
    public static final String KEY_COLUMNS = "columns";
    public static final String KEY_CONTENT = "cnt";
    public static final String KEY_CTE_CYCLE = "cte_cycle";
    public static final String KEY_CTE_MARKER_CYCLE_COLUMN = "column_marker";
    public static final String KEY_CTE_MARKER_CYCLE_VALUE = "value_marker";
    public static final String KEY_CTE_MARKER_CYCLE_VALUE_DEFAULT = "value_default";
    public static final String KEY_CTE_COLUMN_PATH = "column_path";
    public static final String KEY_DISTINCT = "distinct";
    // поле (TParameter), заполняемое в клиенте в процессе подготовки задания на выполнение отчёта
    public static final String KEY_DYNAMIC_INFO = "dynamic_info";
    public static final String KEY_ELSE = "else";
    public static final String KEY_ESCAPE = "esc";
    public static final String KEY_EXPRESSION = "expr";
    public static final String KEY_FIELDS = "fields";
    public static final String KEY_FROM = "from";
    public static final String KEY_FTS_SORT_BY_RESULT = "sort_by_result";
    public static final String KEY_GROUPBY = "groupby";
    public static final String KEY_GROUP_HAVING = "having";
    public static final String KEY_HINT = "hint";
    public static final String KEY_INTO_VARIABLES = "variables";
    public static final String KEY_ITEMS = "items";
    public static final String KEY_JOIN = "join";
    public static final String KEY_JOIN_ON = "on";
    public final static String KEY_KIND = "kind";
    public static final String KEY_LEFT_EXPRESSION = "left";
    public static final String KEY_NOT = "not";
    public static final String KEY_OPERATION = "op";
    public static final String KEY_ORDERBY = "orderby";
    public static final String KEY_ORDERBY_NULL_ORDERING = "null ordering";
    public static final String KEY_ORDERBY_DESC = "desc";
    public static final String KEY_PARAM_NAME = "param_name";
    public static final String KEY_PARAM_TYPE = "param_type";
    public static final String KEY_PARAM_VALUE = "param_value";
    public static final String KEY_PARAMS_CLAUSE = "params_clause";
    public static final String KEY_PREDICATES = "predicates";
    public static final String KEY_REGEXPR_PARAMS = "params";
    public static final String KEY_RETURNING = "returning";
    public static final String KEY_RIGHT_EXPRESSION = "right";
    public static final String KEY_FETCH_FIRST = "rowlimit";
    public static final String KEY_FETCH_OFFSET = "offset";
    public static final String KEY_TABLE = "table";
    public static final String KEY_TEMPLATE = "template";
    public static final String KEY_THEN = "then";
    public static final String KEY_TUPLE = "tuple";
    public static final String KEY_SELECT = "select";
    public static final String KEY_UNION_TYPE = "union_type";
    public static final String KEY_UNIONS = "unions";
    public static final String KEY_WHERE = "where";
    public static final String KEY_WHEN = "when";
    public static final String KEY_WHEN_SET = "when_set";
    public static final String KEY_WITH = "with";
    public static final String KEY_VALUE_TYPE = "type";
    public static final String KEY_VALUES = "values";
}
