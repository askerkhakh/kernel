package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.QueryParams;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderParamsClause extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        JSONArray jsonQueryParams = getArray(node, KEY_CONTENT);
        if (jsonQueryParams == null || jsonQueryParams.size() == 0)
            return null;

        QueryParams queryParams = new QueryParams();
        for (int i = 0; i < jsonQueryParams.size(); i++)
            queryParams.addQueryParam(
                    (QueryParam) JsonSqlParser.parseJsonNode(getObject(jsonQueryParams, i, KEY_PARAMS_CLAUSE))
            );

        return queryParams;
    }

    @Override
    public String getKind() { return KIND_PARAMS; }

}
