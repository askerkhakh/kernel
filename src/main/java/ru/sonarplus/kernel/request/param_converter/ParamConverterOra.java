package ru.sonarplus.kernel.request.param_converter;

import ru.sonarplus.kernel.BytesUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class ParamConverterOra extends ParamConverterDefault {

    public ParamConverterOra(boolean useStandardNulls) {
        super(useStandardNulls);
    }

    @Override
    protected void prepareParamValueTypesMap() {
        super.prepareParamValueTypesMap();
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_BOOLEAN, Types.VARCHAR);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_BYTE, Types.VARCHAR);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_DATE, Types.TIMESTAMP);
        fieldTypeIdToJDBCTypeMap.put(FieldTypeId.tid_TIME, Types.TIMESTAMP);
    }

    @Override
    public Object internalParamValueToJavaValue(Object value, FieldTypeId valueType) {
        switch (valueType) {
            case tid_BOOLEAN:
                return ((Boolean) value) ? "01" : "00";
            case tid_BYTE:
                return BytesUtils.bytesNotEmptyToHexString(new byte[]{((Byte) value).byteValue()}, true);
            // поскольку в Oracle нет отдельных типов для даты и времени, а нас не устраивает стандартная
            // конвертация (если таковая пристутсвует в jdbc драйвере для Oracle) в дату-время с нулевой частсью
            // даты или времени, то выполним конвертацию сами
            case tid_DATE:
                return Timestamp.valueOf(LocalDateTime.of((LocalDate) value, ValuesSupport.SONAR_ZERO_TIME));
            case tid_TIME:
                return Timestamp.valueOf(LocalDateTime.of(ValuesSupport.SONAR_ZERO_DATE, (LocalTime) value));
        }
        return super.internalParamValueToJavaValue(value, valueType);
    }
}
