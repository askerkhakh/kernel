package ru.sonarplus.kernel.request.param_converter;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.BytesUtils;
import ru.sonarplus.kernel.QueryParameterDirection;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;

import javax.inject.Named;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ParamConverterDefault {

    private final boolean useStandardNulls;

    private static final Map<QueryParam.ParamType, QueryParameterDirection> paramTypeToParamDir = new ConcurrentHashMap<>();
    static {
        paramTypeToParamDir.put(QueryParam.ParamType.INPUT, QueryParameterDirection.IN);
        paramTypeToParamDir.put(QueryParam.ParamType.OUTPUT, QueryParameterDirection.OUT);
        paramTypeToParamDir.put(QueryParam.ParamType.INPUT_OUTPUT, QueryParameterDirection.IN_OUT);
        paramTypeToParamDir.put(QueryParam.ParamType.UNKNOWN, QueryParameterDirection.IN);
    }

    public static QueryParameterDirection paramTypeToOraParamDirection(QueryParam.ParamType paramType) {
        QueryParameterDirection res = paramTypeToParamDir.get(paramType);
        if (res == null)
            throw new ParamConverterException(String.format("Тип параметра '%s' не поддержан", paramType));
        return res;
    }

    public Map<FieldTypeId, Integer> fieldTypeIdToJDBCTypeMap = new HashMap<>();

    public ParamConverterDefault(boolean useStandardNulls) {
        this.useStandardNulls = useStandardNulls;
        prepareParamValueTypesMap();
    }

    protected void prepareParamValueTypesMap() {
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_BLOB, Types.OTHER);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_MEMO, Types.OTHER);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_DATE, Types.DATE);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_TIME, Types.TIME);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_DATETIME, Types.TIMESTAMP);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_FLOAT, Types.DOUBLE);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_INTEGER, Types.INTEGER);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_LARGEINT, Types.BIGINT);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_SMALLINT, Types.SMALLINT);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_WORD, Types.INTEGER);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_STRING, Types.VARCHAR);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_UNKNOWN, Types.OTHER);

        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_BOOLEAN, Types.BOOLEAN);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_BYTE, Types.INTEGER);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_CODE, Types.VARCHAR);
    }

    public int fieldTypeIdToJDBCTypes(FieldTypeId valueType)
            throws ParamConverterException{
        Integer res = this.fieldTypeIdToJDBCTypeMap.get(valueType);
        if (res == null)
            throw new ParamConverterException(String.format("Тип значения параметра '%s' не поддержан", valueType));
        return res;
    }

    public final Object paramValueToJavaValue(Object value, FieldTypeId valueType) {
        if (value == null) {
            if (!useStandardNulls) {
                value = ValuesSupport.getZeroValue(valueType);
            } else {
                return null;
            }
        }
        return internalParamValueToJavaValue(value, valueType);
    }

    public Object internalParamValueToJavaValue(Object value, FieldTypeId valueType) {
        switch (valueType) {
            case tid_BOOLEAN:
                return value;

            case tid_BYTE:
                return ((Number)value).byteValue();

            case tid_CODE:
                return BytesUtils.bytesNotEmptyToHexString(((CodeValue) value).getValue(), false);

            case tid_DATE:
                return Date.valueOf((LocalDate) value);

            case tid_TIME:
                return Time.valueOf((LocalTime) value);

            case tid_DATETIME:
                return Timestamp.valueOf((LocalDateTime) value);

            case tid_FLOAT:
                return ((Number)value).doubleValue();

            case tid_INTEGER:
                return ((Number)value).intValue();

            case tid_LARGEINT:
                return ((Number)value).longValue();

            case tid_SMALLINT:
                return ((Number)value).shortValue();

            case tid_STRING:
            case tid_MEMO:
                return value;

            case tid_WORD:
                return ((Number)value).intValue();

            case tid_BLOB:
                if (!(value instanceof byte[]))
                    throw new ParamConverterException(String.valueOf(valueType) + " -> " + value.getClass().getSimpleName());                Preconditions.checkState(value instanceof byte[], value.getClass().getSimpleName());
                return value;

            default:
                throw new ParamConverterException(String.format("Тип параметра '%s' не поддержан", String.valueOf(valueType)));
        }
    }

}
