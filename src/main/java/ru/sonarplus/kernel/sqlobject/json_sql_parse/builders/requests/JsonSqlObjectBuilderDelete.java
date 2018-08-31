package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryDelete;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderDelete extends JsonSqlObjectBuilderRequest {

    @Override
    protected SqlQuery buildRequest(JSONObject node)
            throws Exception {
        SqlQueryDelete request = new SqlQueryDelete();
        String tableName = getIdentifier(node, KEY_TABLE);
        Preconditions.checkState(!StringUtils.isEmpty(tableName));
        request.table = tableName;

        JSONArray jsonReturning = getArray(node, KEY_RETURNING);
        if (jsonReturning != null && jsonReturning.size() != 0)
            for (int i = 0; i < jsonReturning.size(); i++)
                request.getReturning()
                        .insertItem(JsonSqlParser.parseJsonNode(getObject(jsonReturning, i, KEY_RETURNING)));

        JSONArray jsonIntoVariables = getArray(node, KEY_INTO_VARIABLES);
        if (jsonIntoVariables != null && jsonIntoVariables.size() != 0)
            for (int i = 0; i < jsonReturning.size(); i++)
                request.intoVariables.add(getIdentifier(jsonReturning, i));

        JSONObject jsonWhere = getObject(node, KEY_WHERE);
        if (jsonWhere != null) {
            // TODO Раздел WHERE зпросов SELECT/UPDATE/DELETE сериализуется без KIND-а. Наверное нехорошо.
            Conditions predicate = (Conditions) JsonSqlParser.parseJsonNode(jsonWhere, KIND_CONDITIONS);
            if (!predicate.isEmpty())
                request.setWhere(predicate);
        }
        return request;
    }

    @Override
    public String getKind() { return KIND_DELETE; }

}
