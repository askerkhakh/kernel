package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.common_utils.DoubleQuoteUtils.doubleDequoteIdentifier;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_KIND;

public abstract class JsonSqlObjectBuilder {
    private static String INVALID_JSON_OBJECT_KEY = "Неверное значение для ключа '%s' - ожидается %s";
    private static String INVALID_JSON_ARRAY_ITEM = "Неверное значение для %s-го элемента списка '%s' - ожидается %s";

    public abstract SqlObject parseJsonNode(JSONObject node)
            throws Exception;

    protected static String getNodeKind(JSONObject node) {
        String kind = (String) node.get(KEY_KIND);
        Preconditions.checkState(!StringUtils.isEmpty(kind));
        return kind;
    }

    public abstract String getKind();

    protected static boolean getBoolean(JSONObject node, String key) {
        Object value = node.get(key);
        if (value == null)
            return false;
        return (Boolean) value;
    }

    protected static Number getNumber(JSONObject node, String key) {
        Object value = node.get(key);
        if (value == null)
            return null;
        return (Number) value;
    }

    protected static JSONObject getObject(JSONObject node, String key) throws JsonSqlParser.JsonSqlObjectBuilderException {
        Object value = node.get(key);
        if (value != null) {
            if (value instanceof JSONObject)
                return (JSONObject) value;
            else
                throw new JsonSqlParser.JsonSqlObjectBuilderException(
                        String.format(INVALID_JSON_OBJECT_KEY, getNodeKind(node) + "." + key, "объект")
                );
        }
        else
            return null;
    }

    protected static JSONObject getObject(JSONArray node, int index, String arrayKey) throws JsonSqlParser.JsonSqlObjectBuilderException {
        Object value = Preconditions.checkNotNull(node.get(index));
        if (value instanceof JSONObject)
            return (JSONObject) value;
        else
            throw new JsonSqlParser.JsonSqlObjectBuilderException(
                    String.format(INVALID_JSON_ARRAY_ITEM, Integer.toString(index), arrayKey, "объект")
            );
    }

    protected static JSONArray getArray(JSONObject node, String key) throws JsonSqlParser.JsonSqlObjectBuilderException {
        Object value = node.get(key);
        if (value != null) {
            if (value instanceof JSONArray)
                return (JSONArray) value;
            else
                throw new JsonSqlParser.JsonSqlObjectBuilderException(String.format(INVALID_JSON_OBJECT_KEY, key, "список"));
        }
        else
            return null;
    }

    protected static String getIdentifier(JSONObject node, String key) {
        Object value = node.get(key);
        if (value == null)
            return null;
        return doubleDequoteIdentifier((String) value);
    }

    protected static String getIdentifier(JSONArray node, int index) {
        return doubleDequoteIdentifier((String) node.get(index));
    }

    protected static String getIdentifier(String ident) {
        return doubleDequoteIdentifier(ident);
    }
}
