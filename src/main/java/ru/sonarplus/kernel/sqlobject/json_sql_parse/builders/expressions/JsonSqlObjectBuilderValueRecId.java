package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.common_utils.RecIdValueWrapper;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;
import ru.sonarplus.kernel.sqlobject.objects.ValueRecId;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_ITEMS;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_VALUE_RECID;

public class JsonSqlObjectBuilderValueRecId extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        RecIdValueWrapper recId = new RecIdValueWrapper();
        JSONArray jsonRecIdItems = getArray(node, KEY_ITEMS);
        ValueConst valueConst;
        if (jsonRecIdItems != null && jsonRecIdItems.size() != 0)
            for (int i = 0; i < jsonRecIdItems.size(); i++ ) {
                valueConst = (ValueConst) JsonSqlParser.parseJsonNode(getObject(jsonRecIdItems, i, KEY_ITEMS));
                recId.add(valueConst.getValue());
            }
        return new ValueRecId(recId);
    }

    @Override
    public String getKind() { return KIND_VALUE_RECID;}

}
