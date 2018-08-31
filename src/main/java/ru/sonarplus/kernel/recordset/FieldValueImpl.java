package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalTime;

import static java.util.Objects.requireNonNull;

public class FieldValueImpl implements FieldValue {

    @Nullable
    private final Object value;
    private final FieldTypeId typeId;
    @Nullable
    private final DataTypeSpec typeSpec;

    private FieldValueImpl(@Nullable Object value, FieldTypeId typeId, @Nullable DataTypeSpec typeSpec) {
        this.typeId = typeId;
        this.typeSpec = typeSpec;
        if (value == null)
            this.value = null;
        else
            switch (typeId) {
                case tid_BYTE:
                case tid_WORD:
                case tid_SMALLINT:
                case tid_INTEGER:
                case tid_LARGEINT:
                    this.value = ((Number) value).longValue();
                    break;
                default:
                    this.value = value;
            }
    }

    FieldValueImpl(@Nullable Object value, FieldTypeId typeId) {
        this(value, typeId, null);
    }

    FieldValueImpl(@Nullable Object value, DataTypeSpec typeSpec) {
        this(value, typeSpec.getFieldTypeId(), typeSpec);
    }

    @Override
    public FieldTypeId getTypeId() {
        return typeId;
    }

    @Override
    public LocalDate asDate() {
        return (LocalDate) requireNonNull(value);
    }

    @Override
    public LocalTime asTime() {
        return (LocalTime) requireNonNull(value);
    }

    @Override
    @Nullable
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return (value == null);
    }
}
