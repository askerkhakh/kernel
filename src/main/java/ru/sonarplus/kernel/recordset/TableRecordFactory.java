package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.SqlObjectExecutionService;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.record_selector.RecordSetSelectorFactory;


/**
 *  Фабрика для записи таблицы. Позволяет скрыть реализацию от пользователей {@link TableRecord}.
 */
public class TableRecordFactory {

    /**
     * Строит пустую запись таблицы по описателю таблицы
     * @param tableSpec - описатель таблицы
     * @return - запись таблицы
     */
    public static TableRecord ofTableSpec(TableSpec tableSpec) {
        return new TableRecordImpl(tableSpec);
    }

    /**
     * Позволяет получить запись таблицы по значению первичного или уникального ключа
     * @param sqlObjectExecutionService - механизм исполнения SqlObject-запросов
     * @param session - сессия, в рамках которой выполняется запрос на получение записи
     * @param tableName - имя таблицы
     * @param keyName - имя поля первичного или уникального ключа
     * @param keyValue - значение первичного или уникального ключа
     * @return - запись таблицы, если записи с таким ключом нет в таблице - исключение.
     * @throws Exception - заглушка для борьбы с checked-исключениями
     */
    public static TableRecord ofPrimaryKey(SqlObjectExecutionService sqlObjectExecutionService, ClientSession session,
                                           String tableName, String keyName, FieldValue keyValue) throws Exception {
        TableSpec tableSpec = session.getDbSchemaSpec().getTableSpecByName(tableName);
        TableRecordImpl tableRecord = new TableRecordImpl(tableSpec);
        try(
            RecordSet recordSet = RecordSetSelectorFactory.newInstance(sqlObjectExecutionService)
                    .selectColumns(getColumnsByTableSpec(tableSpec))
                    .from(tableName)
                    .addKeyValuePair(keyName, keyValue)
                    .selectNotEmpty(session)
        ) {
            for (int i = 0; i < tableSpec.getFieldSpecCount(); i++) {
                tableRecord.getField(i).setValue(recordSet.getField(i).getValue());
                tableRecord.getField(i).setOldValue(recordSet.getField(i).getValue());
            }
        }

        return tableRecord;
    }

    private static String[] getColumnsByTableSpec(TableSpec tableSpec) {
        String[] columns = new String[tableSpec.getFieldSpecCount()];
        for (int i = 0; i < tableSpec.getFieldSpecCount(); i++)
            columns[i] = tableSpec.getFieldSpec(i).getFieldName();
        return columns;
    }
}
