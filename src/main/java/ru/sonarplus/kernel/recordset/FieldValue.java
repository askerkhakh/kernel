package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.FieldTypeId;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Значение поля. Содержит непосредственно значение в нативном типе Java и тип значения.
 * Аналог записи FieldValue в Delphi поддержке.
 */
public interface FieldValue {

    FieldTypeId getTypeId();

    LocalDate asDate();

    LocalTime asTime();

    @Nullable
    Object getValue();

    boolean isNull();

    default boolean equalsTo(@Nullable Object value) {
        Object thisValue = getValue();
        if (value instanceof FieldValue)
            if (thisValue == null)
                return (((FieldValue) value).getValue() == null);
            else
                return thisValue.equals(((FieldValue) value).getValue());
        else
            if (thisValue == null)
                return (value == null);
            else
                if (value == null)
                    return false;
                switch (getTypeId()) {
                    case tid_BYTE:
                    case tid_WORD:
                    case tid_SMALLINT:
                    case tid_INTEGER:
                    case tid_LARGEINT:
                        // у нас в поле целочисленные типы в виде Long, а для сравнения прийти может в любом, поэтому
                        // приводим к long. Хорошо бы, конечно, здесь использовать приведение с контролем (что-то вроде
                        // longValueExact), но на уровне Number ничего такого нет, поэтому пока так.
                        return thisValue.equals(((Number) value).longValue());
                default:
                    // TODO поддержать остальные типы
                    throw new AssertionError();
                }
    }
}
