package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.TableSpec;

public class TableRecordImpl extends RecordImp implements TableRecord {

    private final TableSpec tableSpec;

    TableRecordImpl(TableSpec tableSpec) {
        this.tableSpec = tableSpec;
        for (FieldSpec fieldSpec : tableSpec.getFields()) {
            this.addField(new FieldImpl(fieldSpec));
        }
    }

    @Override
    public TableSpec getTableSpec() {
        return tableSpec;
    }

}
