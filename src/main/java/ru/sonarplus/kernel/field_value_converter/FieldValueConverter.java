package ru.sonarplus.kernel.field_value_converter;

import ru.sonarplus.kernel.dbschema.FieldTypeId;

import javax.annotation.Nullable;

/*
FIXME Дублирует FieldValueConvertor. Чтобы прямо сейчас не рефакторить FieldValueConvertor делаю почти дубль.
Возможно, там где сейчас используется FieldValueConvertor, следует использовать RecordSet.
*/
public interface FieldValueConverter {

    @Nullable
    default Object convert(@Nullable Object objectFromResultSet, FieldTypeId fieldTypeId) {
        return objectFromResultSet;
    }

}
