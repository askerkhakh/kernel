package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.ParamRef;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_CONTENT;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_PARAMETER;

public class JsonSqlObjectBuilderParamRef extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node) {
        String paramName = getIdentifier(node, KEY_CONTENT);
        Preconditions.checkState(!StringUtils.isEmpty(paramName));
        return new ParamRef(paramName);
    }

    @Override
    public String getKind() { return KIND_PARAMETER ;}

}