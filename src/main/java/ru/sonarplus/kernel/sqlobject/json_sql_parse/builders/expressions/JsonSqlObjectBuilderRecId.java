package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_ALIAS;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_RECID;

public class JsonSqlObjectBuilderRecId extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node) {
        return new QualifiedRField(getIdentifier(node, KEY_ALIAS), null);
    }

    @Override
    public String getKind() { return KIND_RECID;}

}
