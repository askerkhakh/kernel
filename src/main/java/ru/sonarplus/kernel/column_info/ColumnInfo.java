package ru.sonarplus.kernel.column_info;

import ru.sonarplus.kernel.dbschema.FieldSpec;

import javax.annotation.Nullable;

/**
 * Информация о колонке выборки
 */
public interface ColumnInfo {

    @Nullable
    FieldSpec getFieldSpec();

    String getFieldName();

}