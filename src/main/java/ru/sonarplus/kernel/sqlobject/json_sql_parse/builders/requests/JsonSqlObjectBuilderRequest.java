package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.QueryParams;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_HINT;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_PARAMS_CLAUSE;

public abstract class JsonSqlObjectBuilderRequest extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        SqlQuery request = buildRequest(node);

        JSONObject jsonQueryParams = getObject(node, KEY_PARAMS_CLAUSE);
        if (jsonQueryParams != null && jsonQueryParams.size() != 0)
            request.setParams(buildParamsClause(jsonQueryParams));
        request.hint = (String) node.get(KEY_HINT);

        return request;
    }

    protected abstract SqlQuery buildRequest(JSONObject node)
            throws Exception;

    protected QueryParams buildParamsClause(JSONObject jsonQueryParams)
            throws Exception {
        return (QueryParams) JsonSqlParser.parseJsonNode(jsonQueryParams);
    }
}
