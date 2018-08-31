package ru.sonarplus.kernel.record_selector;

import com.google.common.base.Verify;
import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.recordset.FieldValue;
import ru.sonarplus.kernel.recordset.RecordSet;

/**
 * Объект, позволяющий получить набор записей, удовлетворяющий набору условий ключ=значение.
 * Настроечные методы возвращают исходный интерфейс, что позволяет использовать method-chaining.
 * Аналог DataSetSelector из Delphi. При желании можно разглядеть паттерн строитель (builder).
 */
public interface RecordSetSelector {

    /**
     * Позволяет указать какие колонки должны попасть в итоговый набор записей.
     * @param columns - имена полей
     * @return - исходный интерфейс для method-chaining'а.
     */
    RecordSetSelector selectColumns(String... columns);

    /**
     * Позволяет указать таблицу, в которой выполняется выборка записей
     * @param table - имя таблицы
     * @return - исходный интерфейс для method-chaining'а.
     */
    RecordSetSelector from(String table);

    /**
     * Позволяет указать пару ключ=значение
     * @param fieldName - имя поля ключа
     * @param fieldValue - значение поля ключа
     * @return - исходный интерфейс для method-chaining'а.
     */
    RecordSetSelector addKeyValuePair(String fieldName, FieldValue fieldValue);

    /**
     * Возвращает набор записей таблицы, заданной в {@link RecordSetSelector#from(String)}, с полями, указанными в
     * {@link RecordSetSelector#selectColumns(String...)}, удовлетворяющий условиям, заданным в
     * {@link RecordSetSelector#addKeyValuePair(String, FieldValue)}.
     * @param session - сессия, в рамках которой выполняется выборка записей.
     * @return - результирующий набор записей.
     * @throws Exception - борьба с checked-исключениями.
     */
    RecordSet select(ClientSession session) throws Exception;

    /**
     * То же что и {@link RecordSetSelector#select(ClientSession)}, только в случае пустоты полученной выборки
     * выбрасывает исключение.
     * @param session - сессия, в рамках которой выполняется выборка записей.
     * @return - результирующий набор записей.
     * @throws Exception - борьба с checked-исключениями.
     */
    default RecordSet selectNotEmpty(ClientSession session) throws Exception {
        RecordSet recordSet = select(session);
        // TODO этот ужасный try/catch можно попробовать обобщить и вынести в функцию
        try {
            Verify.verify(!recordSet.isEmpty());
        }
        catch (Throwable e) {
            try {
                recordSet.close();
            }
            catch (Throwable closeException) {
                e.addSuppressed(closeException);
                throw e;
            }
            throw e;
        }
        return recordSet;
    }
}
