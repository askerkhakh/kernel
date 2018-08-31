package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.column_info.ColumnInfo;
import ru.sonarplus.kernel.field_value_converter.FieldValueConverter;

import java.sql.ResultSet;

/**
 * Фабрика для набора записей. Позволяет скрыть реализацию от пользователей {@link RecordSet}.
 */
public class RecordSetFactory {
    public static RecordSet newInstance(ResultSet resultSet, ColumnInfo[] columnInfoArray, FieldValueConverter fieldConverter) throws Exception {
        return new RecordSetImpl(resultSet, columnInfoArray, fieldConverter);
    }
}
