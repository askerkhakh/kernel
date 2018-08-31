package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static ru.sonarplus.kernel.dbschema.FieldTypeId.*;

public class ValueConst extends Value {
    private Object value = null;
    private FieldTypeId valueType = tid_UNKNOWN;

    private ValueConst() { super(); }

    public ValueConst(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

    public ValueConst(CodeValue value) {
        this();
        this.value = value;
        this.valueType = tid_CODE;
    }

    public ValueConst(Boolean value) {
        this();
        this.value = value;
        this.valueType = tid_BOOLEAN;
    }

    public ValueConst(Byte value) {
        this();
        this.value = value;
        this.valueType = tid_BYTE;
    }

    public ValueConst(Short value) {
        this();
        this.value = value;
        this.valueType = tid_SMALLINT;
    }

    public ValueConst(Integer value) {
        this();
        this.value = value;
        this.valueType = tid_INTEGER;
    }

    public ValueConst(Long value) {
        this();
        this.value = value;
        this.valueType = tid_LARGEINT;
    }

    public ValueConst(Float value) {
        this();
        this.value = value;
        this.valueType = tid_FLOAT;
    }

    public ValueConst(Double value) {
        this();
        this.value = value;
        this.valueType = tid_FLOAT;
    }

    public ValueConst(String value) {
        this();
        this.value = value;
        this.valueType = tid_STRING;
    }

    public static ValueConst ofDateTime(LocalDateTime dateTime) {
        ValueConst value = new ValueConst();
        value.value = dateTime;
        value.valueType = tid_DATETIME;
        return value;
    }

    public static ValueConst ofDate(LocalDate date) {
        ValueConst value = new ValueConst();
        value.value = date;
        value.valueType = tid_DATE;
        return value;
    }

    public static ValueConst ofTime(LocalTime time) {
        ValueConst value = new ValueConst();
        value.value = time;
        value.valueType = tid_TIME;
        return value;
    }

    public ValueConst(@Nullable Object value, FieldTypeId typeId)
            throws ValuesSupport.ValueException {
        this();

        this.valueType = typeId;
        if (value != null)
            this.value = ValuesSupport.castValue(value, getValueType());
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
    public void setValue(Object value) throws ValuesSupport.ValueException {
        if (value == null)
            // тип значения не меняем если он был ранее установлен
            this.value = null;
        else {
            ValuesSupport.checkValue(value);
            if (getValueType() == tid_UNKNOWN)
                // тип значения ранее не был определён - определим сейчас
                this.valueType = ValuesSupport.defineValueType(value);
            this.value = ValuesSupport.castValue(value, getValueType());
        }
	}

	@Override
	public FieldTypeId getValueType() {
        if (valueType == null)
            valueType = tid_UNKNOWN;
		return valueType;
	}

	@Override
	public void setValueType(FieldTypeId value) throws ValuesSupport.ValueException {
        if (this.valueType == value)
            return;
        FieldTypeId fromType = this.valueType;
        this.valueType = value;
        if (this.value != null)
            // ранее было установлено значение. теперь ему поменяли тип - попробуем сконвертировать значение
           this.value = ValuesSupport.castValue(this.value, fromType, getValueType());
	}

    public static ValueConst createNull() {
        return new ValueConst();
    }

}
