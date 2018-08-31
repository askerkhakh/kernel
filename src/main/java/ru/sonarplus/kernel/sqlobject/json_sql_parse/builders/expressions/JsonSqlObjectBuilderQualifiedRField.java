package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_CONTENT;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_FIELDRNAME;

public class JsonSqlObjectBuilderQualifiedRField extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node) {
        QualifiedName qname = QualifiedName.stringToQualifiedName((String) node.get(KEY_CONTENT));
        return new QualifiedRField(getIdentifier(qname.alias), getIdentifier(qname.name));
    }

    @Override
    public String getKind() { return KIND_FIELDRNAME ;}
}
