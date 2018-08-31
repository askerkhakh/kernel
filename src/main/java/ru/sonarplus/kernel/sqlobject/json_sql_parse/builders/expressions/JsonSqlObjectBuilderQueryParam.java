package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.Value;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderQueryParam extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        String paramName = Preconditions.checkNotNull(getIdentifier(node, KEY_PARAM_NAME));
        QueryParam.ParamType paramType = QueryParam.ParamType.fromString((String) node.get(KEY_PARAM_TYPE));
        if (paramType == null)
            paramType = QueryParam.ParamType.UNKNOWN;
        JSONObject jsonParamValue = getObject(node, KEY_PARAM_VALUE);
        Value paramValue = null;
        if (jsonParamValue != null)
            paramValue = (Value) JsonSqlParser.parseJsonNode(jsonParamValue);
        QueryParam result = new QueryParam(paramName, paramValue, paramType);
        result.dynamicInfo = (String) node.get(KEY_DYNAMIC_INFO);
        return result;
    }

    @Override
    public String getKind() { return KIND_PARAMSTATIC ;}

}
