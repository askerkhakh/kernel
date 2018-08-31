package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import ru.sonarplus.kernel.sqlobject.objects.Join;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_JOIN_LEFT;

public class JsonSqlObjectBuilderJoinLeft extends JsonSqlObjectBuilderJoin{

    @Override
    protected Join.JoinType getJoinType() { return Join.JoinType.LEFT; }

    @Override
    public String getKind() { return KIND_JOIN_LEFT; }

}
