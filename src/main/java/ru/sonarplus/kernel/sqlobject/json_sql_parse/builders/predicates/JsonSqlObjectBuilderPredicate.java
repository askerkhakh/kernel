package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_NOT;

public abstract class JsonSqlObjectBuilderPredicate extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        Predicate predicate = (Predicate) Preconditions.checkNotNull(internalParseJsonNode(node));
        predicate.not = getBoolean(node, KEY_NOT);
        return predicate;
    }

    protected abstract SqlObject internalParseJsonNode(JSONObject node)
            throws Exception;
}
