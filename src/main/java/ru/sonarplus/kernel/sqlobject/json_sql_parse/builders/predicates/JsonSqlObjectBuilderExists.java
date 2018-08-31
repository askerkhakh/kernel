package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.PredicateExists;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_SELECT;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_EXISTS;

public class JsonSqlObjectBuilderExists extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_EXISTS; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        PredicateExists predicate = new PredicateExists();
        predicate.setSelect((Select) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(node, KEY_SELECT))));
        return predicate;
    }

}
