package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.*;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderUpdate extends JsonSqlObjectBuilderRequest {

    @Override
    protected SqlQuery buildRequest(JSONObject node)
            throws Exception {
        SqlQueryUpdate request = new SqlQueryUpdate();
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

        JSONArray jsonUpdateFields = getArray(node, KEY_FIELDS);
        Preconditions.checkState(jsonUpdateFields != null && jsonUpdateFields.size() != 0);

        JSONArray jsonUpdateValues = getArray(node, KEY_VALUES);
        Preconditions.checkState(jsonUpdateValues != null && jsonUpdateValues.size() != 0);

        Preconditions.checkState(jsonUpdateFields.size() == jsonUpdateValues.size());

        for (int i = 0; i < jsonUpdateFields.size(); i++) {
            request.set(
                    (QualifiedField) JsonSqlParser.parseJsonNode(getObject(jsonUpdateFields, i, KEY_FIELDS)),
                    (ColumnExpression) JsonSqlParser.parseJsonNode(getObject(jsonUpdateValues, i, KEY_VALUES))) ;
        }

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
    public String getKind() { return KIND_UPDATE; }

}
