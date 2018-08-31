package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.GroupBy;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.TupleExpressions;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderGroupBy extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        JSONArray jsonItems = getArray(node, KEY_ITEMS);
        JSONObject jsonHaving = getObject(node, KEY_GROUP_HAVING);

        TupleExpressions items = null;
        Conditions having = null;
        if (jsonItems != null && jsonItems.size() != 0) {
            items = new TupleExpressions();
            for (int i = 0; i < jsonItems.size(); i++)
                items.insertItem(JsonSqlParser.parseJsonNode(getObject(jsonItems, i, getKind() + "." + KEY_ITEMS)));
        }

        if (jsonHaving != null)
            having = (Conditions) JsonSqlParser.parseJsonNode(jsonHaving, KIND_CONDITIONS);

        if ((items != null && items.itemsCount() != 0) || (having != null && !having.isEmpty())) {
            GroupBy groupBy = new GroupBy();
            groupBy.insertItem(items);
            groupBy.setHaving(having);
            return groupBy;
        }
        return null;
    }

    @Override
    public String getKind() { return KIND_GROUPBY; }

}
