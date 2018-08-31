package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.*;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderInsert extends JsonSqlObjectBuilderRequest {

    @Override
    protected SqlQuery buildRequest(JSONObject node)
            throws Exception {
        SqlQueryInsert request = new SqlQueryInsert();
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

        JSONArray jsonInsertFields = getArray(node, KEY_FIELDS);
        Preconditions.checkState(jsonInsertFields != null && jsonInsertFields.size() != 0);


        JSONObject jsonInsertSelect = getObject(node, KEY_SELECT);
        JSONArray jsonInsertValues = getArray(node, KEY_VALUES);

        if (jsonInsertSelect != null) {
            Preconditions.checkState(jsonInsertValues == null);
            request.setSelect((Select) JsonSqlParser.parseJsonNode(getObject(node, KEY_SELECT)));
        }
        else {
            Preconditions.checkState(jsonInsertValues != null && jsonInsertValues.size() != 0);
            Preconditions.checkState(jsonInsertFields.size() == jsonInsertValues.size());
            DMLFieldAssignment fieldAssignment;
            for (int i = 0; i < jsonInsertFields.size(); i++) {
                fieldAssignment = new DMLFieldAssignment(request.newAssignments());
                fieldAssignment.setField((QualifiedField) JsonSqlParser.parseJsonNode(getObject(jsonInsertFields, i, KEY_FIELDS)));
                fieldAssignment.setExpr((ColumnExpression) JsonSqlParser.parseJsonNode(getObject(jsonInsertValues, i, KEY_VALUES)));
            }
        }
        return request;
    }

    @Override
    public String getKind() { return KIND_INSERT; }

}
