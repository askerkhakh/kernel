package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

public abstract class Value extends ColumnExpression {

	public Value() { super(); }

	public Value(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
	public abstract Object getValue();
	public abstract void setValue(Object value) throws ValuesSupport.ValueException;
	
	public abstract FieldTypeId getValueType();
	public abstract void setValueType(FieldTypeId value) throws ValuesSupport.ValueException;
	
	public boolean isNull() {
		return getValue() == null;
	}
	
	public CodeValue getCodeValue() {
		Preconditions.checkState(getValueType() == FieldTypeId.tid_CODE,
				"Значение не является кодом кодификатора");
		return new CodeValue((byte[]) getValue());
	}
	

}
