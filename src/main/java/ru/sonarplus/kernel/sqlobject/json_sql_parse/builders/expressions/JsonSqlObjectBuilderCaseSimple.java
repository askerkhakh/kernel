package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import com.google.common.base.Preconditions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.CaseSimple;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderCaseSimple extends JsonSqlObjectBuilder {
    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        JSONArray jsonWhenThenSet = Preconditions.checkNotNull(getArray(node, KEY_WHEN_SET));
        Preconditions.checkState(jsonWhenThenSet.size() != 0);
        JSONObject jsonCaseExpr = Preconditions.checkNotNull(getObject(node, KEY_CASE_EXPR));
        JSONObject jsonElse = getObject(node, KEY_ELSE);
        CaseSimple caseSimple = new CaseSimple();
        caseSimple.setCase((ColumnExpression) JsonSqlParser.parseJsonNode(jsonCaseExpr));
        buildWhenThenSet(jsonWhenThenSet, caseSimple);
        caseSimple.setElse((ColumnExpression) JsonSqlParser.parseJsonNode(jsonElse));
        return caseSimple;
    }

    @Override
    public String getKind() { return KIND_CASE_SIMPLE; }

    protected void buildWhenThenSet(JSONArray jsonWhenThenSet, CaseSimple caseSimple)
            throws Exception {
        for (int i = 0; i < jsonWhenThenSet.size(); i++) {
            JSONObject jsonWhenThen = getObject(jsonWhenThenSet, i, KEY_WHEN_SET);
            new CaseSimple.WhenThen(caseSimple,
                    (ColumnExpression) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(jsonWhenThen, KEY_WHEN))),
                    (ColumnExpression) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(jsonWhenThen, KEY_THEN))));
        }
    }

}
