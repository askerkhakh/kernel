package ru.sonarplus.kernel.recordset;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.column_info.ColumnInfo;
import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.field_value_converter.FieldValueConverter;

import java.sql.ResultSet;


class RecordSetImpl implements RecordSet {

    private final ResultSet resultSet;
    private final FieldValueConverter converter;
    private final boolean empty;
    private Record record;

    RecordSetImpl(ResultSet resultSet, ColumnInfo[] columnInfoArray, FieldValueConverter converter) throws Exception {
        Preconditions.checkArgument(resultSet.getMetaData().getColumnCount() == columnInfoArray.length);
        this.resultSet = resultSet;
        this.converter = converter;
        // запись и её структуру создаём сразу
        record = new RecordImp();
        for (ColumnInfo columnInfo : columnInfoArray) {
            ((RecordImp) record).addField(columnInfo.getFieldName(), new FieldImpl(columnInfo.getFieldSpec()));
        }
        empty = !resultSet.next();
        if (!empty) {
            fillRecord();
        }
    }

    private void fillRecord() throws Exception {
        for (int i = 0; i < getFieldCount(); i++) {
            Field field = getField(i);
            DataTypeSpec dataTypeSpec = field.getFieldSpec().getDataTypeSpec();
            field.setValue(
                    FieldValueFactory.ofJavaValue(
                            converter.convert(resultSet.getObject(i + 1), dataTypeSpec.getFieldTypeId()),
                            dataTypeSpec
                    )
            );
        }
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public Field tryGetFieldByName(String fieldName) {
        return record.tryGetFieldByName(fieldName);
    }

    @Override
    public Iterable<Field> getFields() {
        return record.getFields();
    }

    @Override
    public int getFieldCount() {
        return record.getFieldCount();
    }

    @Override
    public Field getField(int index) {
        return record.getField(index);
    }

    @Override
    public void close() throws Exception {
        resultSet.close();
    }
}
