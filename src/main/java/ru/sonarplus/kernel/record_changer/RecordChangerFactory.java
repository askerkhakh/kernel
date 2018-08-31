package ru.sonarplus.kernel.record_changer;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.data_change_service.DataChangeService;
import ru.sonarplus.kernel.recordset.TableRecord;

/**
 * Фабрика для построения {@link RecordChanger}. Изолирует реализацию последнего от его пользователей.
 */
public class RecordChangerFactory {

    public static RecordChanger ofTableRecord(DataChangeService dataChangeService, ClientSession session, TableRecord tableRecord) {
        return new RecordChangerImpl(dataChangeService, session, tableRecord);
    }

}
