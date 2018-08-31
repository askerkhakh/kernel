package ru.sonarplus.kernel.field_value_converter;

import ru.sonarplus.kernel.dbschema.FieldTypeId;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class FieldValueConverterOracle extends FieldValueConverterAbstract {

    public FieldValueConverterOracle(boolean useSonarNulls) {
        super(useSonarNulls);
    }

    @Override
    public Object internalConvert(Object objectFromResultSet, FieldTypeId fieldTypeId) {
        switch (fieldTypeId) {
            case tid_DATE:
            case tid_TIME:
            case tid_DATETIME: {
                Timestamp timestamp = (Timestamp) objectFromResultSet;
                switch (fieldTypeId) {
                    case tid_DATE:
                        return timestamp.toLocalDateTime().toLocalDate();
                    case tid_TIME:
                        return timestamp.toLocalDateTime().toLocalTime();
                    case tid_DATETIME:
                        return timestamp.toLocalDateTime();
                }
            }
            // все простые целочисленные типы держим в виде long
            case tid_BYTE:
                return (long) (((byte[]) objectFromResultSet)[0] & 0xFF);
            case tid_WORD:
            case tid_SMALLINT:
            case tid_INTEGER:
            case tid_LARGEINT:
                // целочисленные типы, кроме байта представлены в Oracle в виде NUMBER, который jdbc превращает в BidGecimal
                return ((BigDecimal) objectFromResultSet).longValueExact();
            case tid_BOOLEAN:
                return ((byte[]) objectFromResultSet)[0] != 0;
            // TODO поддержать остальные типы
            default:
                return objectFromResultSet;
        }
    }

}
