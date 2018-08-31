package ru.sonarplus.kernel.dbvalues;

import ru.sonarplus.kernel.dbschema.FieldSpec;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by stepanov on 30.11.2017.
 */
abstract public class ADBField {
    public String lFieldName;
    public String rFieldName;
    public String sqlCommand;
    public List<Object> sqlCommandParams;
    @Nullable
    private final FieldSpec fieldSpec;


    public ADBField(String lFieldName, String rFieldName) {
        this(lFieldName, rFieldName, null);
    }

    public ADBField(String lFieldName, String rFieldName, @Nullable FieldSpec fieldSpec) {
        this.lFieldName = lFieldName;
        this.rFieldName = rFieldName;
        this.fieldSpec = fieldSpec;
    }

    public abstract DBFieldValue getValue();
    public abstract void setValue(DBFieldValue value);

    public abstract DBFieldValue getOldValue();
    public abstract void setOldValue(DBFieldValue value);

    public String getName() {
        return rFieldName;
    }

    @Nullable
    public FieldSpec getFieldSpec() {
        return fieldSpec;
    }

}
