package ru.sonarplus.kernel.dbvalues;

import ru.sonarplus.kernel.dbschema.FieldSpec;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by stepanov on 29.11.2017.
 */
public class DBField extends ADBField {

    private DBFieldValue value;
    private DBFieldValue oldValue;

    public DBField(String lFieldName, String rFieldName, @Nullable DBFieldValue value, @Nullable FieldSpec fieldSpec) {
        super(lFieldName, rFieldName, fieldSpec);
        if (value == null) {
            this.value = DBFieldValue.fromJavaValue(null, null, null);
        }
        else {
            this.value = value;
        }
    }
    public DBField(String lFieldName, String rFieldName, DBFieldValue value) {
        this(lFieldName, rFieldName, value, null);
    }

    public static DBField fromEnum(String lName, DBFieldValue.IEnumDBFieldValue value) {
        return new DBField(lName, "", value.toDBValue());
    }

    public static DBField sqlCommand(String lName, String sql, Object... params) {
        DBField res = new DBField(lName, "", null);
        res.sqlCommand = sql;
        res.sqlCommandParams = Arrays.asList(params);
        return res;
    }

    @Override
    public DBFieldValue getValue() {
        return value;
    }

    @Override
    public void setValue(DBFieldValue value) {
        this.value = value;
    }

    @Override
    public DBFieldValue getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(DBFieldValue value) {
        this.oldValue = value;
    }

    public void setDate(Date dateValue) {
        value = DBFieldValue.fromJavaValue(dateValue, value.typeId, value.typeSpec);
    }

    public void setLong(Long longValue) {
        value = DBFieldValue.fromJavaValue(longValue, value.typeId, value.typeSpec);
    }

    public void setString(String strValue) {
        value = DBFieldValue.fromJavaValue(strValue, value.typeId, value.typeSpec);
    }

    public void setStream(InputStream stream) {
        value = DBFieldValue.fromJavaValue(stream, value.typeId, value.typeSpec);
    }

    @Override
    public String toString() {
        return lFieldName + ": " + (value == null? "": value.toString());
    }
}
