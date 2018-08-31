package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.DbSchemaUtils;
import ru.sonarplus.kernel.dbschema.TableSpec;

import static java.util.Objects.requireNonNull;

/**
 * Запись таблицы. В дополнение к Record имеет TableSpec.
 */
public interface TableRecord extends Record {

    TableSpec getTableSpec();

    default String getTableName() {
        return getTableSpec().getName();
    }

    default Field getPrimaryKeyField() {
        return getFieldByName(requireNonNull(DbSchemaUtils.getPrimaryKeyFieldSpec(getTableSpec())).getFieldName());
    }
}
