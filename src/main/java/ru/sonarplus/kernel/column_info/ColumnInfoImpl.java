package ru.sonarplus.kernel.column_info;

import ru.sonarplus.kernel.dbschema.FieldSpec;

import javax.annotation.Nullable;

class ColumnInfoImpl implements ColumnInfo{

    @Nullable
    private FieldSpec fieldSpec;
    private String fieldName;

    @Nullable
    public FieldSpec getFieldSpec() {
        return fieldSpec;
    }

    public void setFieldSpec(@Nullable FieldSpec fieldSpec) {
        this.fieldSpec = fieldSpec;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    ColumnInfoImpl() {}

}
