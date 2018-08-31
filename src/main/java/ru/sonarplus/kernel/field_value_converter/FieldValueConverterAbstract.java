package ru.sonarplus.kernel.field_value_converter;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

import javax.annotation.Nullable;

abstract class FieldValueConverterAbstract implements FieldValueConverter {

    private final boolean useSonarNulls;

    FieldValueConverterAbstract(boolean useSonarNulls) {
        this.useSonarNulls = useSonarNulls;
    }

    @Nullable
    @Override
    public Object convert(@Nullable Object objectFromResultSet, FieldTypeId fieldTypeId) {
        if (objectFromResultSet == null)
            return null;
        Object converted = internalConvert(objectFromResultSet, fieldTypeId);
        if (useSonarNulls && objectFromResultSet.equals(ValuesSupport.getZeroValue(fieldTypeId)))
            return null;
        return converted;
    }

    protected abstract Object internalConvert(Object objectFromResultSet, FieldTypeId fieldTypeId);
}
