package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import com.google.common.base.Preconditions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.CaseSearch;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderCaseSearch extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        JSONArray jsonWhenThenSet = Preconditions.checkNotNull(getArray(node, KEY_WHEN_SET));
        Preconditions.checkState(jsonWhenThenSet.size() != 0);
        JSONObject jsonElse = getObject(node, KEY_ELSE);
        CaseSearch caseSearch = new CaseSearch();
        buildWhenThenSet(jsonWhenThenSet, caseSearch);
        caseSearch.setElse((ColumnExpression) JsonSqlParser.parseJsonNode(jsonElse));
        return caseSearch;
    }

    @Override
    public String getKind() { return KIND_CASE_SEARCH; }

    protected void buildWhenThenSet(JSONArray jsonWhenThenSet, CaseSearch caseSearch)
            throws Exception {
        for (int i = 0; i < jsonWhenThenSet.size(); i++) {
            JSONObject jsonWhenThen = getObject(jsonWhenThenSet, i, KEY_WHEN_SET);
            new CaseSearch.WhenThen(caseSearch,
                    (Predicate) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(jsonWhenThen, KEY_WHEN))),
                    (ColumnExpression) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(jsonWhenThen, KEY_THEN))));
        }
    }
}
