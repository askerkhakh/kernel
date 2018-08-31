package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.CallStoredProcedure;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderCallSP extends JsonSqlObjectBuilderRequest {

    @Override
    protected SqlQuery buildRequest(JSONObject node)
            throws Exception {
        CallStoredProcedure request = new CallStoredProcedure();
        String procName = getIdentifier(node, KEY_CONTENT);
        Preconditions.checkState(!StringUtils.isEmpty(procName));
        request.spName = procName;

        JSONArray jsonArgs = getArray(node, KEY_ARGS);
        if (jsonArgs != null && jsonArgs.size() != 0)
            for (int i = 0; i < jsonArgs.size(); i++)
                request.newTuple()
                        .insertItem(JsonSqlParser.parseJsonNode(getObject(jsonArgs, i, KEY_ARGS)));
        return request;
    }

    @Override
    public String getKind() { return KIND_SP; }

}
