package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderConditions extends JsonSqlObjectBuilderPredicate {

    private static final String NOT = "not ";
    private static final String OR = "or";

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {

        Conditions predicate = (Conditions) internalParseJsonNode(node);
        String strOp = (String) node.get(KEY_OPERATION);

        if (StringUtils.isEmpty(strOp)) {
            predicate.not = false;
            predicate.booleanOp = Conditions.BooleanOp.AND;
        }
        else {
            predicate.not = StringUtils.startsWithIgnoreCase(strOp, NOT);
            predicate.booleanOp = StringUtils.endsWithIgnoreCase(strOp, OR) ? Conditions.BooleanOp.OR : Conditions.BooleanOp.AND;
        }
        return predicate;
    }

    @Override
    public String getKind() { return KIND_CONDITIONS; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {

        JSONArray jsonPredicates = getArray(node, KEY_PREDICATES);
        Conditions predicate = new Conditions();
        for (int i = 0; i < jsonPredicates.size(); i++)
            predicate.insertItem(
                    JsonSqlParser.parseJsonNode(
                            getObject(jsonPredicates, i, getKind() + "." + KEY_PREDICATES)));

        return predicate;
    }

}
