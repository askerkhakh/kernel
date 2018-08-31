package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;

import javax.xml.bind.DatatypeConverter;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderValueConst extends JsonSqlObjectBuilder {

    private static final String VALUE_TYPE_NOT_SUPPORTED = "Тип значения '%s' не поддержан";

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        FieldTypeId valueType = Preconditions.checkNotNull(FieldTypeId.fromString((String) node.get(KEY_VALUE_TYPE)));
        Object cnt = Preconditions.checkNotNull(node.get(KEY_CONTENT));
        return buildValueConst(node.toJSONString(), cnt, valueType);
    }

    @Override
    public String getKind() { return KIND_VALUE; }

    // TODO можно было-бы попробовать кешировать sql-объекты со значениями,
    // чтобы снова и снова не заниматься преобразованиями json-строк в значения.
    // Но хотя множество значений и счётно - их может оказаться слишком много.
    //private static final Map<String, ValueConst> cacheValues = new HashMap<>();

    protected ValueConst buildValueConst (String jsonString, Object cnt, FieldTypeId valueType)
            throws Exception {
        //ValueConst result = cacheValues.get(jsonString);
        //if (result != null)
        //    return (ValueConst)result.clone();
        return new ValueConst(buildValue(cnt, valueType), valueType);
    }

    protected Object buildValue(Object cnt, FieldTypeId valueType)
            throws Exception {
        if (cnt instanceof String && cnt.equals("DMJQO721E#NULL"))
            return null;
        switch (valueType) {
            case tid_BOOLEAN:
                return Boolean.valueOf((String)cnt);
            case tid_STRING:
            case tid_MEMO:
                return cnt;
            case tid_DATE:
                return ValuesSupport.parseDate((String) cnt);
            case tid_TIME:
                return ValuesSupport.parseTime((String) cnt);
            case tid_DATETIME:
                return ValuesSupport.parseDateTime((String) cnt);
            case tid_CODE:
                return CodeValue.valueOf((String)cnt);
            case tid_FLOAT:
                return ((Number)cnt).doubleValue();
            case tid_BYTE:
                return ((Number)cnt).byteValue();
            case tid_SMALLINT:
                return ((Number)cnt).shortValue();
            case tid_INTEGER:
            case tid_WORD:
                return ((Number)cnt).intValue();
            case tid_LARGEINT:
                return ((Number)cnt).longValue();
            case tid_BLOB:
                return DatatypeConverter.parseHexBinary((String)cnt);
            default:
                throw new JsonSqlParser.JsonSqlObjectBuilderException(String.format(VALUE_TYPE_NOT_SUPPORTED, valueType.toString()));
        }
    }
}
