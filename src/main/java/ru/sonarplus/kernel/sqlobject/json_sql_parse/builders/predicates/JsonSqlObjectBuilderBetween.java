package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateBetween;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderBetween extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_BETWEEN; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {

        JSONObject jsonExpr = Preconditions.checkNotNull(getObject(node, KEY_EXPRESSION));
        JSONObject jsonLeft = Preconditions.checkNotNull(getObject(node, KEY_LEFT_EXPRESSION));
        JSONObject jsonRight = Preconditions.checkNotNull(getObject(node, KEY_RIGHT_EXPRESSION));
        PredicateBetween predicate = new PredicateBetween();
        predicate.setExpr((ColumnExpression) JsonSqlParser.parseJsonNode(jsonExpr));
        predicate.setLeft((ColumnExpression) JsonSqlParser.parseJsonNode(jsonLeft));
        predicate.setRight((ColumnExpression) JsonSqlParser.parseJsonNode(jsonRight));
        return predicate;
    }

}
