package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateInQuery;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;


public class JsonSqlObjectBuilderInSelect extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_IN_SELECT; }

    @Override
    protected SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        JSONArray jsonTuple = Preconditions.checkNotNull(getArray(node, KEY_TUPLE));
        Preconditions.checkState(jsonTuple.size() != 0);
        PredicateInQuery predicate = new PredicateInQuery();
        predicate.setSelect(
                (Select)JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(node, KEY_SELECT)))
        );
        for (int i = 0; i < jsonTuple.size(); i++)
            predicate.tupleAdd(
                    (ColumnExpression) JsonSqlParser.parseJsonNode(
                            getObject(jsonTuple, i, getKind() + "." + KEY_TUPLE)));
        return predicate;
    }

}
