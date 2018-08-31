package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.*;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderCursorSpec extends JsonSqlObjectBuilderRequest {

    private static final String UNKNOWN_NULL_ORDERING_OPTION = "Неизвестная опция для сортировки по nulls";

    @Override
    protected SqlQuery buildRequest(JSONObject node)
            throws Exception {
        CursorSpecification cursor = new CursorSpecification();

        JSONObject jsonSelect = getObject(node, KEY_SELECT);
        cursor.setSelect((Select) JsonSqlParser.parseJsonNode(Preconditions.checkNotNull(jsonSelect), KIND_SELECT));
        cursor.setFetchFirst(getLongValue(node, KEY_FETCH_FIRST));
        cursor.setFetchOffset(getLongValue(node, KEY_FETCH_OFFSET));

        JSONArray jsonOrderBy = getArray(node, KEY_ORDERBY);
        if (jsonOrderBy != null && jsonOrderBy.size() != 0)
            parseOrderBy(jsonOrderBy, cursor);

        return cursor;
    }

    protected long getLongValue(JSONObject node, String key) {
        Number num = getNumber(node, key);
        if (num == null)
            return 0;
        long value = num.longValue();
        if (value >= 0)
            return value;
        return 0;
    }

    @Override
    public String getKind() { return KIND_CURSOR_SPEC; }

    protected void parseOrderBy(JSONArray jsonOrderBy, CursorSpecification cursor)
            throws Exception {

        OrderBy orderBy = new OrderBy(cursor);
        for (int i = 0; i < jsonOrderBy.size(); i++) {
            OrderByItem orderByItem = new OrderByItem(orderBy);
            orderByItem.setExpr((ColumnExpression) JsonSqlParser.parseJsonNode(getObject(jsonOrderBy, i, KEY_ORDERBY)));
            JSONObject jsonOrderByItem = getObject(jsonOrderBy, i, KEY_ORDERBY);
            String nullOrdering = (String) jsonOrderByItem.get(KEY_ORDERBY_NULL_ORDERING);
            if (StringUtils.isEmpty(nullOrdering))
                orderByItem.nullOrdering = OrderByItem.NullOrdering.NONE;
            else if (nullOrdering.compareToIgnoreCase("nulls first") == 0)
                orderByItem.nullOrdering = OrderByItem.NullOrdering.NULLS_FIRST;
            else if (nullOrdering.compareToIgnoreCase("nulls last") == 0)
                orderByItem.nullOrdering = OrderByItem.NullOrdering.NULLS_LAST;
            else
                throw new JsonSqlParser.JsonSqlObjectBuilderException(UNKNOWN_NULL_ORDERING_OPTION);

            if (getBoolean(jsonOrderByItem, KEY_ORDERBY_DESC))
                orderByItem.direction = OrderByItem.OrderDirection.DESC;
        }
    }

}
