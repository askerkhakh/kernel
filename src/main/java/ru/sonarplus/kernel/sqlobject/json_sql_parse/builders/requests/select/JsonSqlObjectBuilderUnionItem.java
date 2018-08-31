package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.UnionItem;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderUnionItem extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        UnionItem unionItem = new UnionItem();
        unionItem.unionType = UnionItem.UnionType.fromString((String) node.get(KEY_UNION_TYPE));
        unionItem.setSelect((Select) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(node, KEY_SELECT))));
        return unionItem;
    }

    @Override
    public String getKind() { return KIND_UNION_ITEM; }

}
