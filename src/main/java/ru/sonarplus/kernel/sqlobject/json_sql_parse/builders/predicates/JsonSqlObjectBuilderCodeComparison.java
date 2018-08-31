package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateForCodeComparison;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderCodeComparison extends JsonSqlObjectBuilderPredicate {

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {

        JSONObject jsonLeft = Preconditions.checkNotNull(JsonSqlObjectBuilder.getObject(node, KEY_LEFT_EXPRESSION));
        JSONObject jsonRight = Preconditions.checkNotNull(JsonSqlObjectBuilder.getObject(node, KEY_RIGHT_EXPRESSION));
        PredicateForCodeComparison predicate = new PredicateForCodeComparison();
        predicate.setLeft((ColumnExpression) JsonSqlParser.parseJsonNode(jsonLeft));
        predicate.setRight((ColumnExpression) JsonSqlParser.parseJsonNode(jsonRight));
        predicate.comparison = PredicateForCodeComparison.ComparisonCodeOperation.fromString((String) node.get(KEY_OPERATION));
        return predicate;
    }

    @Override
    public String getKind() { return KIND_CODE_COMPARISON; }

}
