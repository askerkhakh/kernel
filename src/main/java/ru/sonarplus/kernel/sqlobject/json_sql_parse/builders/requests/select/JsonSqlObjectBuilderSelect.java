package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import com.google.common.base.Preconditions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.JsonSqlObjectBuilderRequest;
import ru.sonarplus.kernel.sqlobject.objects.*;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;


public class JsonSqlObjectBuilderSelect extends JsonSqlObjectBuilderRequest {

    @Override
    public SqlQuery buildRequest(JSONObject node)
            throws Exception {
        Select select = new Select();

        select.distinct = getBoolean(node, KEY_DISTINCT);
        parseWith(node, select);
        parseColumns(node, select);
        parseFrom(node, select);
        parseWhere(node, select);
        parseGroupBy(node, select);
        parseUnions(node, select);

        return select;
    }

    @Override
    public String getKind() { return KIND_SELECT; }

    protected void parseWith(JSONObject node, Select select)
            throws Exception {
        JSONArray jsonWith = getArray(node, KEY_WITH);
        if (jsonWith != null) {
            CTEsContainer with = new CTEsContainer(select);
            for (int i = 0; i < jsonWith.size(); i++)
                parseCTE(getObject(jsonWith, i, KEY_WITH), with);
        }
    }

    protected void parseCTE(JSONObject node, CTEsContainer with)
            throws Exception {
        JSONArray jsonColumns = Preconditions.checkNotNull(getArray(node, KEY_COLUMNS));
        CommonTableExpression cte = new CommonTableExpression(with);
        cte.alias = getIdentifier(node, KEY_ALIAS);
        cte.setSelect((Select) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(node, KEY_SELECT))));
        cte.setCycle((CTECycleClause) JsonSqlParser.parseJsonNode(getObject(node, KEY_CTE_CYCLE)));
        for (int i = 0; i < jsonColumns.size(); i++)
            cte.columns.add(getIdentifier(jsonColumns, i));
    }

    protected void parseColumns(JSONObject node, Select select)
            throws Exception {
        JSONArray jsonColumns = getArray(node, KEY_COLUMNS);
        if (jsonColumns != null) {
            SelectedColumnsContainer columns = new SelectedColumnsContainer(select);
            for (int i = 0; i < jsonColumns.size(); i++)
                parseColumn(getObject(jsonColumns, i, KEY_COLUMNS), columns);
        }
    }

    protected void parseColumn(JSONObject node, SelectedColumnsContainer columns)
            throws Exception {
        new SelectedColumn(columns)
                .setExpression((ColumnExpression) JsonSqlParser.parseJsonNode(node))
                .alias = getIdentifier(node, KEY_ALIAS);
    }

    protected void parseFrom(JSONObject node, Select select)
            throws Exception {
        JSONArray jsonFrom = getArray(node, KEY_FROM);
        if (jsonFrom != null) {
            FromContainer from = new FromContainer(select);
            for (int i = 0; i< jsonFrom.size(); i++)
                parseFromItem(getObject(jsonFrom, i, KEY_FROM), from);
        }
    }

    protected void parseFromItem(JSONObject node, FromContainer from)
            throws Exception {
        new FromClauseItem(from,
                new TableExpression(
                        (Source) JsonSqlParser.parseJsonNode(node),
                        getIdentifier(node, KEY_ALIAS)
                ),
                (Join) JsonSqlParser.parseJsonNode(getObject(node, KEY_JOIN))
        );
    }

    protected void parseWhere(JSONObject node, Select select)
            throws Exception {
        JSONObject jsonWhere = getObject(node, KEY_WHERE);
        if (jsonWhere != null) {
            // TODO Раздел WHERE зпросов SELECT/UPDATE/DELETE сериализуется без KIND-а. Наверное нехорошо.
            Conditions conditions = (Conditions) JsonSqlParser.parseJsonNode(jsonWhere, KIND_CONDITIONS);
            if (!conditions.isEmpty())
                select.setWhere(conditions);
        }
    }

    protected void parseGroupBy(JSONObject node, Select select)
            throws Exception {
        JSONObject jsonGroupBy = getObject(node, KEY_GROUPBY);
        if (jsonGroupBy != null)
            select.setGroupBy((GroupBy) JsonSqlParser.parseJsonNode(jsonGroupBy));
    }

    protected void parseUnions(JSONObject node, Select select)
            throws Exception {
        JSONArray jsonUnions = getArray(node, KEY_UNIONS);
        if (jsonUnions != null && jsonUnions.size() != 0) {
            for (int i = 0; i < jsonUnions.size(); i++)
                select.newUnions().insertItem(JsonSqlParser.parseJsonNode(getObject(jsonUnions, i, KEY_UNIONS)));
        }
    }
}
