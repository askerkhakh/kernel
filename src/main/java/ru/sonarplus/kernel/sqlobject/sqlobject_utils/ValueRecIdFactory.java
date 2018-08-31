package ru.sonarplus.kernel.sqlobject.sqlobject_utils;

import ru.sonarplus.kernel.dbschema.DbSchemaUtils;
import ru.sonarplus.kernel.recordset.Field;
import ru.sonarplus.kernel.recordset.TableRecord;
import ru.sonarplus.kernel.sqlobject.common_utils.RecIdValueWrapper;
import ru.sonarplus.kernel.sqlobject.objects.ValueRecId;

import static java.util.Objects.requireNonNull;

public class ValueRecIdFactory {

    public static ValueRecId ofTableRecord(TableRecord record) {
        for (Field field : record.getFields()) {
            if (DbSchemaUtils.isUniquField(field.getFieldSpec())) {
                // TODO пока поддерживаем только простой случай идентификатора записи - когда это уникальное поле
                RecIdValueWrapper recId = new RecIdValueWrapper();
                recId.add(requireNonNull(requireNonNull(field.getValue()).getValue()));
                return new ValueRecId(recId);
            }
        }
        throw new AssertionError();
    }

}
