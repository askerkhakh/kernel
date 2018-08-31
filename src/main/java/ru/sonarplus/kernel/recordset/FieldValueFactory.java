package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Фабрика для построения {@link FieldValue}. Содержит статические методы-конструкторы. Позволяет скрыть реализацию от
 * пользователей {@link FieldValue}.
 */
public class FieldValueFactory {

    public static FieldValue ofLong(Long longValue) {
        return new FieldValueImpl(longValue, FieldTypeId.tid_LARGEINT);
    }

    public static FieldValue ofJavaValue(@Nullable Object value, DataTypeSpec typeSpec) {
        return new FieldValueImpl(value, typeSpec);
    }

    public static FieldValue ofDate(LocalDate date) {
        return new FieldValueImpl(date, FieldTypeId.tid_DATE);
    }

    public static FieldValue ofTime(LocalTime time) {
        return new FieldValueImpl(time, FieldTypeId.tid_TIME);
    }
}
