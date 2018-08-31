package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateLike;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderLike extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_LIKE; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        PredicateLike predicate = new PredicateLike();
        predicate.setLeft((ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_EXPRESSION))));
        predicate.setRight((ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_TEMPLATE))));
        predicate.escape = (String) node.get(KEY_ESCAPE);
        return predicate;
    }

}
