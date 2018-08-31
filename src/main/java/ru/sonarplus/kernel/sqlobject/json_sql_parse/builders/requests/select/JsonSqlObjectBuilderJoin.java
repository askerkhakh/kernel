package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.Join;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_JOIN_ON;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_CONDITIONS;

public abstract class JsonSqlObjectBuilderJoin extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        Join join = new Join();
        //#BAD# Завязка на TConditions, TODO нужно избавиться от завязки на TConditions
        join.setJoinOn((Predicate) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(getObject(node, KEY_JOIN_ON)), KIND_CONDITIONS));
        join.joinType = getJoinType();
        return join;
    }

    protected abstract Join.JoinType getJoinType();
}
