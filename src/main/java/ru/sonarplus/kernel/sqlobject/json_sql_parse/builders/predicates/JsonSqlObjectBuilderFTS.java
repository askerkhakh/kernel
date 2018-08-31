package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateFullTextSearch;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderFTS extends JsonSqlObjectBuilderPredicate{

    @Override
    public String getKind() { return KIND_FULLTEXTSEARCH; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        PredicateFullTextSearch predicate = new PredicateFullTextSearch();
        predicate.setLeft((ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_EXPRESSION))));
        predicate.setRight((ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_TEMPLATE))));
        predicate.sortByResult = getBoolean(node, KEY_FTS_SORT_BY_RESULT);
        return predicate;
    }

}
