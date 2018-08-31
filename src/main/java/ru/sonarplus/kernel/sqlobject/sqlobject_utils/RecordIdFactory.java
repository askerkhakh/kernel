package ru.sonarplus.kernel.sqlobject.sqlobject_utils;

import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;

public class RecordIdFactory {
    public static ColumnExpression newRecordId() {
        return new QualifiedRField("", "");
    }
}
