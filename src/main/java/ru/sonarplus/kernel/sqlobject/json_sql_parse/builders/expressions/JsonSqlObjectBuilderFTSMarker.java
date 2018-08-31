package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.OraFTSMarker;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_FULLTEXTSEARCH_MARKER;

public class JsonSqlObjectBuilderFTSMarker extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node) {
        return new OraFTSMarker();
    }

    @Override
    public String getKind() { return KIND_FULLTEXTSEARCH_MARKER;}

}
