package ru.sonarplus.kernel.sqlobject.json_sql_parse;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions.*;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.predicates.*;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.*;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select.*;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.transactions.*;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlParser {

    private final static String BUILDER_FOR_KIND_NOT_REGISTERED = "Не зарегистрирован SqlObject-построитель для json-элемента '%s'";

    public static class JsonSqlObjectBuilderException extends Exception{
        public JsonSqlObjectBuilderException(String message) {
            super(message);
        }
    }

    private final static Map<String, JsonSqlObjectBuilder> builderMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    static  {
        try {
            registerBuilder(JsonSqlObjectBuilderCaseSearch.class);
            registerBuilder(JsonSqlObjectBuilderCaseSimple.class);
            registerBuilder(JsonSqlObjectBuilderExpression.class);
            registerBuilder(JsonSqlObjectBuilderFTSMarker.class);
            registerBuilder(JsonSqlObjectBuilderQualifiedField.class);
            registerBuilder(JsonSqlObjectBuilderQualifiedRField.class);
            registerBuilder(JsonSqlObjectBuilderParamRef.class);
            registerBuilder(JsonSqlObjectBuilderQueryParam.class);
            registerBuilder(JsonSqlObjectBuilderValueConst.class);
            registerBuilder(JsonSqlObjectBuilderRecId.class);
            registerBuilder(JsonSqlObjectBuilderValueRecId.class);

            registerBuilder(JsonSqlObjectBuilderBetween.class);
            registerBuilder(JsonSqlObjectBuilderComparison.class);
            registerBuilder(JsonSqlObjectBuilderConditions.class);
            registerBuilder(JsonSqlObjectBuilderExists.class);
            registerBuilder(JsonSqlObjectBuilderInSelect.class);
            registerBuilder(JsonSqlObjectBuilderInTuple.class);
            registerBuilder(JsonSqlObjectBuilderIsNull.class);
            registerBuilder(JsonSqlObjectBuilderLike.class);
            registerBuilder(JsonSqlObjectBuilderRegExpLike.class);
            registerBuilder(JsonSqlObjectBuilderCodeComparison.class);
            registerBuilder(JsonSqlObjectBuilderFTS.class);

            registerBuilder(JsonSqlObjectBuilderCursorSpec.class);

            registerBuilder(JsonSqlObjectBuilderSelect.class);
            registerBuilder(JsonSqlObjectBuilderSourceSelect.class);
            registerBuilder(JsonSqlObjectBuilderSourceTable.class);
            registerBuilder(JsonSqlObjectBuilderGroupBy.class);
            registerBuilder(JsonSqlObjectBuilderCTECycle.class);
            registerBuilder(JsonSqlObjectBuilderUnionItem.class);
            registerBuilder(JsonSqlObjectBuilderJoinLeft.class);
            registerBuilder(JsonSqlObjectBuilderJoinRight.class);
            registerBuilder(JsonSqlObjectBuilderJoinInner.class);
            registerBuilder(JsonSqlObjectBuilderJoinFullOuter.class);

            registerBuilder(JsonSqlObjectBuilderDelete.class);
            registerBuilder(JsonSqlObjectBuilderUpdate.class);
            registerBuilder(JsonSqlObjectBuilderInsert.class);
            registerBuilder(JsonSqlObjectBuilderCallSP.class);

            registerBuilder(JsonSqlObjectBuilderTRSRemoveSavePoint.class);
            registerBuilder(JsonSqlObjectBuilderTRSRollback.class);
            registerBuilder(JsonSqlObjectBuilderTRSRollBackTo.class);
            registerBuilder(JsonSqlObjectBuilderTRSSetSavePoint.class);
            registerBuilder(JsonSqlObjectBuilderTRSSetTransaction.class);
            registerBuilder(JsonSqlObjectBuilderTRSCommit.class);

            registerBuilder(JsonSqlObjectBuilderParamsClause.class);
        }
        catch (IllegalAccessException |InstantiationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected static void registerBuilder(Class builderClass)
            throws IllegalAccessException, InstantiationException {
        JsonSqlObjectBuilder builderInstance = (JsonSqlObjectBuilder) builderClass.newInstance();
        builderMap.put(builderInstance.getKind(), builderInstance);
    }

    private final static Map<String, JSONParser> jsonParsers = new HashMap<>();

    protected static JSONParser getJSONParser(String sessionId) {
        JSONParser jsonParser = jsonParsers.get(sessionId);
        if (jsonParser == null) {
            jsonParser = new JSONParser();
            if (!StringUtils.isEmpty(sessionId))
                jsonParsers.put(sessionId, jsonParser);
        }
        return jsonParser;
    }

    public static void forget(String sessionId) {
        jsonParsers.remove(sessionId);
    }

    public static SqlObject parseJsonString(String json, String sessionId)
            throws Exception {
        return parseJsonNode((JSONObject) getJSONParser(sessionId).parse(json));
    }

    public static SqlObject parseJsonString(String json)
            throws Exception {
        return parseJsonString(json, null);
    }

    public static SqlObject parseJsonNode(JSONObject jsonNode, String kind)
            throws Exception {
        if (jsonNode == null)
            return null;
        Preconditions.checkState(!StringUtils.isEmpty(kind));
        JsonSqlObjectBuilder parser;
        if (kind.compareToIgnoreCase(KIND_SELECT) == 0) {
            //#BAD# "Подпрыгивания" вызванные совпадением в Delphi значений stKindSelect и stKindSourceSelect
            if (jsonNode.get(KEY_CONTENT) == null)
                parser = builderMap.get(kind);
            else
                parser = builderMap.get(KIND_SOURCE_SELECT);
        }
        else
            parser = builderMap.get(kind);

        if (parser == null)
            throw new JsonSqlObjectBuilderException(String.format(BUILDER_FOR_KIND_NOT_REGISTERED, kind));
        return parser.parseJsonNode(jsonNode);
    }

    public static SqlObject parseJsonNode(JSONObject jsonNode)
            throws Exception {
        if (jsonNode == null)
            return null;
        String kind = (String) jsonNode.get(KEY_KIND);
        Preconditions.checkState(!StringUtils.isEmpty(kind));
        return parseJsonNode(jsonNode, kind);
    }
}
