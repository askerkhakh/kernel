package ru.sonarplus.kernel.dbvalues;

import java.util.HashMap;
import java.util.Map;

import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.TableSpec;

import javax.annotation.Nullable;

import static com.google.common.base.Verify.verifyNotNull;

/**
 * Created by stepanov on 29.11.2017.
 */
public class DBRow {
    private DBRow owner;
    private Map<String, ADBField> fields = new HashMap<>();
    // <FIX> убрать флажок byLatinName - временный workaround для того, чтобы не переделывать 
    // старый код - в будущем имя поля для СУБД не должно выходить на первый план.
    private final boolean byLatinName;
    private String tableName;
    @Nullable
    private TableSpec tableSpec;

    public DBRow(DBRow owner, boolean byLatinName) {
        this.owner = owner;
        this.byLatinName = byLatinName;
    }

    public DBRow(boolean byLatinName) {
        this.byLatinName = byLatinName;
    }

    public static DBRow ofTableSpec(TableSpec tableSpec) {
        DBRow row = new DBRow(false);
        row.tableSpec = tableSpec;
        row.tableName = tableSpec.getName();
        for (FieldSpec fieldSpec : tableSpec.getFields()) {
            row.addField(new DBField(fieldSpec.getLatinName(), fieldSpec.getFieldName(), null, fieldSpec));
        }
        return row;
    }

    @Nullable
    public TableSpec getTableSpec() {
        return tableSpec;
    }

    public String getTableName() {
        return tableName;
    }

    public Iterable<ADBField> getFields() {
        return fields.values();
    }

    public ADBField[] fieldsArray() {
        return fields.values().toArray(new ADBField[0]);
    }

    public ADBField[] notEmptyFields() {
        return fields.values().stream().filter(fld -> !fld.getValue().isEmpty()).toArray(ADBField[]::new);
    }

    protected <T> T addField(ADBField field) {
        fields.put((byLatinName ? field.lFieldName : field.rFieldName).toUpperCase(), field);
        if (owner != null) {
            owner.addField(field);
        }
        return (T) field;
    }

    protected DBField field(String lName, String rName, FieldTypeId typeId, DataTypeSpec typeSpec) {
        return addField(new DBField(lName, rName, DBFieldValue.fromDB(null, typeId, typeSpec)));
    }

    protected DBField field(String lName, String rName, FieldTypeId typeId) {
        return addField(new DBField(lName, rName, DBFieldValue.fromDB(null, typeId, null)));
    }

    protected DBField field(String lName, FieldTypeId typeId) {
        return addField(new DBField(lName, "", DBFieldValue.fromDB(null, typeId, null)));
    }

    protected DBField id(String lName, FieldTypeId typeId) {
        return addField(new DBField(lName, "", DBFieldValue.fromDB(null, typeId, null)));
    }

    protected <T extends DBFieldValue.IEnumDBFieldValue> EnumDBField<T> enumField(String lName) {
        return addField(new EnumDBField<T>(lName, ""));
    }

    public ADBField tryGetFieldByName(String fieldName) {
        return fields.get(fieldName.toUpperCase());
    }

    public ADBField getFieldByName(String fieldName) {
        return verifyNotNull(tryGetFieldByName(fieldName));
    }

}
