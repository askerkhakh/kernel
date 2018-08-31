package ru.sonarplus.kernel.record_changer;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.data_change_service.DataChangeService;
import ru.sonarplus.kernel.recordset.Field;
import ru.sonarplus.kernel.recordset.FieldValue;
import ru.sonarplus.kernel.recordset.FieldValueFactory;
import ru.sonarplus.kernel.recordset.TableRecord;
import ru.sonarplus.kernel.transaction.Transaction;

public class RecordChangerImpl implements RecordChanger {

    private final DataChangeService dataChangeService;
    private final ClientSession session;
    private final TableRecord tableRecord;

    RecordChangerImpl(DataChangeService dataChangeService, ClientSession session, TableRecord tableRecord) {
        this.dataChangeService = dataChangeService;
        this.session = session;
        this.tableRecord = tableRecord;
    }

    @Override
    public void setFieldValue(String fieldName, Object fieldValue) {
        Field field = tableRecord.getFieldByName(fieldName);
        FieldValue newValue;
        if (fieldValue instanceof FieldValue)
            newValue = (FieldValue) fieldValue;
        else
            newValue = FieldValueFactory.ofJavaValue(fieldValue, field.getFieldSpec().getDataTypeSpec());
        field.setValue(newValue);
    }

    @Override
    public void save() throws Exception {
        Transaction trs = session.newTransaction();
        try {
            if (tableRecord.getPrimaryKeyField().isNull())
                dataChangeService.insertRecord(session, tableRecord, null);
            else
                dataChangeService.modifyRecord(session, tableRecord, null);
            trs.commit();
        }
        catch (Throwable e) {
            trs.rollback();
            throw e;
        }
    }
}
