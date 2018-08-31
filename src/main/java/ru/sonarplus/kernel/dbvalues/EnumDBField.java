package ru.sonarplus.kernel.dbvalues;

/**
 * Created by stepanov on 30.11.2017.
 */
public class EnumDBField<T extends DBFieldValue.IEnumDBFieldValue> extends ADBField {

    public T enumValue;

    public EnumDBField(String lFieldName, String rFieldName) {
        super(lFieldName, rFieldName);
    }

    @Override
    public DBFieldValue getValue() {
        return enumValue.toDBValue();
    }

    @Override
    public void setValue(DBFieldValue value) {}

    @Override
    public DBFieldValue getOldValue() {
        return null;
    }

    @Override
    public void setOldValue(DBFieldValue value) {

    }

    @Override
    public String toString() {
        return lFieldName + ": " + (enumValue != null ? enumValue.toString() : "");
    }

}
