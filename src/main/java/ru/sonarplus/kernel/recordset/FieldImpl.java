package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.FieldSpec;

import javax.annotation.Nullable;

class FieldImpl implements Field {

    private final FieldSpec fieldSpec;
    @Nullable
    private FieldValue value;
    @Nullable
    private FieldValue oldValue;

    FieldImpl(FieldSpec fieldSpec) {
        this.fieldSpec = fieldSpec;
    }

    @Override
    @Nullable
    public FieldValue getValue() {
        return value;
    }

    @Override
    public void setValue(@Nullable FieldValue fieldValue) {
        value = fieldValue;

    }

    @Override
    @Nullable
    public FieldValue getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(@Nullable FieldValue oldFieldValue) {
        oldValue = oldFieldValue;
    }

    @Override
    public FieldSpec getFieldSpec() {
        return fieldSpec;
    }

    @Override
    public boolean isNull() {
        return (value == null) || (value.isNull());
    }

}
