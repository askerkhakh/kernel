package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SourceQuery;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_CONTENT;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_SOURCE_SELECT;

public class JsonSqlObjectBuilderSourceSelect extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        JSONObject jsonContent = Preconditions.checkNotNull(getObject(node, KEY_CONTENT));
        return new SourceQuery(null, (Select) JsonSqlParser.parseJsonNode(jsonContent));
    }

    @Override
    public String getKind() { return KIND_SOURCE_SELECT; }

}
