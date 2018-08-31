package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateIsNull;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_EXPRESSION;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_ISNULL;

public class JsonSqlObjectBuilderIsNull extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_ISNULL; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        // TODO для IsNull поддержать маршаллинг свойства IsRaw
        PredicateIsNull predicate = new PredicateIsNull();
        predicate.setExpr(
                (ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_EXPRESSION)))
        );
        return predicate;
    }

}
