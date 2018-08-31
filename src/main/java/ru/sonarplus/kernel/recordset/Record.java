package ru.sonarplus.kernel.recordset;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Запись. Набор полей с доступом по имени, индексу и с возможностью перебора в цикле for each.
 */
public interface Record {

    @Nullable
    Field tryGetFieldByName(String fieldName);

    default Field getFieldByName(String fieldName) {
        return requireNonNull(tryGetFieldByName(fieldName));
    }

    default FieldValue getFieldValue(String fieldName) {
        return requireNonNull(getFieldByName(fieldName).getValue());
    }

    Iterable<Field> getFields();

    int getFieldCount();

    Field getField(int index);

}
