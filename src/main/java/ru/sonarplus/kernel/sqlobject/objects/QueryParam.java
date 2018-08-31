package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.TreeMap;

public class QueryParam extends Parameter {

    public enum ParamType {
        UNKNOWN ("unknown"),
        INPUT ("input"),
        OUTPUT ("output"),
        INPUT_OUTPUT ("input_output"),
        RESULT ("result");

        private String text;
        ParamType(String text) { this.text = text; }
        static final Map<String, ParamType> PARAM_TYPES_MAP = new TreeMap(String.CASE_INSENSITIVE_ORDER) {
            {
                for (ParamType item: ParamType.values()) {
                    put(item.text, item);
                }
            }
        };

        public static ParamType fromString(String text) {

            return Preconditions.checkNotNull(PARAM_TYPES_MAP.get(text));
        }

        public String toString() {
            return text;
        }

    }

    private ParamType paramType;
    private Value valueObj = ValueConst.createNull();

    public QueryParam(
            String name, Value value,
            ParamType paramType)
            throws SqlObjectException {
        this(null, name, value, paramType);
    }

    public QueryParam(@Nullable SqlObject owner,
                      String name, Value value,
                      ParamType paramType)
            throws SqlObjectException {
        this(owner);
        this.parameterName = name;
        this.paramType = paramType;
        setValueObj(value);
    }

    public QueryParam() { super(); }

    public QueryParam(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

    public boolean isContainedInParamsClause() {
        return owner != null && owner.getClass() == QueryParams.class;
    }

    public ParamType getParamType() { return paramType; }

    public void setParamType(ParamType paramType) { this.paramType = paramType; }

    public Value getValueObj() { return valueObj; }

    public void setValueObj(Value value)
            throws SqlObjectException{
        if (this.valueObj == value)
            return;
        if (value != null) {
            if (value.owner != null)
                value.owner.removeItem(value);
            this.valueObj = value;
        }
        else
            setNull();
    }

    public void setNull() {
        this.valueObj = ValueConst.createNull();
    }

    public boolean isNull() {
        return getValue() == null;
    }

    public Object getValue() {
        return Preconditions.checkNotNull(this.valueObj).getValue();
    }

    public void setValue(Object value)
            throws ValuesSupport.ValueException {
        this.valueObj.setValue(value);
    }

    public FieldTypeId getValueType() {
        return Preconditions.checkNotNull(this.valueObj).getValueType();
    }

    public void setValueType(FieldTypeId value)
            throws ValuesSupport.ValueException {
        Preconditions.checkNotNull(this.valueObj).setValueType(value);
    }

    public boolean isRecId() {
        return this.valueObj != null && this.valueObj.getClass() == ValueRecId.class;
    }

    public ParamRef createParam() {
        return new ParamRef(parameterName);
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        QueryParam targetInstance = (QueryParam) target;
        targetInstance.valueObj = this.valueObj == null ? null : target.setOwner((Value) this.valueObj.clone());
    }
}
