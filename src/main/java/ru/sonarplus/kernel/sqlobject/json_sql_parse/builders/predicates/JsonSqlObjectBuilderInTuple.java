package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateInTuple;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderInTuple extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_IN_TUPLE; }

    @Override
    protected SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        JSONObject jsonExpr = Preconditions.checkNotNull(JsonSqlObjectBuilder.getObject(node, KEY_EXPRESSION));
        JSONArray jsonTuple = Preconditions.checkNotNull(JsonSqlObjectBuilder.getArray(node, KEY_TUPLE));
        Preconditions.checkState(jsonTuple.size() != 0);

        PredicateInTuple predicate = new PredicateInTuple();
        predicate.setExpr((ColumnExpression) JsonSqlParser.parseJsonNode(jsonExpr));
        for (int i = 0; i < jsonTuple.size(); i++)
            predicate.tupleAdd(
                    (ColumnExpression) JsonSqlParser.parseJsonNode(
                            JsonSqlObjectBuilder.getObject(jsonTuple, i, getKind() + "." + KEY_TUPLE)));
        return predicate;
    }

}
