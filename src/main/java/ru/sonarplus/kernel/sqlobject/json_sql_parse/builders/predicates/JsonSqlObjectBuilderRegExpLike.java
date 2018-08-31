package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateRegExpMatch;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderRegExpLike extends JsonSqlObjectBuilderPredicate {

    @Override
    public String getKind() { return KIND_REGEXP_LIKE; }

    @Override
    public SqlObject internalParseJsonNode(JSONObject node)
            throws Exception {
        PredicateRegExpMatch predicate = new PredicateRegExpMatch();
        predicate.setLeft((ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_EXPRESSION))));
        predicate.setRight((ColumnExpression) Preconditions.checkNotNull(JsonSqlParser.parseJsonNode(getObject(node, KEY_TEMPLATE))));
        String params = (String) node.get(KEY_REGEXPR_PARAMS);
        if (!StringUtils.isEmpty(params)) {
            predicate.caseSensitive = (params.indexOf('c') >= 0);
            predicate.pointAsCRLF = (params.indexOf('n') >= 0);
            predicate.multiLine = (params.indexOf('m') >= 0);

        }
        return predicate;
    }

}
