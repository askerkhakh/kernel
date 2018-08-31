package ru.sonarplus.kernel.sqlobject.sqlobject_utils;

import ru.sonarplus.kernel.recordset.FieldValue;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;

public class ValueConstFactory {

    public static ValueConst ofFieldValue(FieldValue fieldValue) {
        return new ValueConst(fieldValue.getValue(), fieldValue.getTypeId());
    }

}
