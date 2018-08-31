package ru.sonarplus.kernel.sqlobject.common_utils;

import ru.sonarplus.kernel.dbschema.FieldTypeId;

public class FieldValue {
	public Object value;
	public FieldTypeId valueType;

	public FieldValue(Object value, FieldTypeId valueType) {
		this.value = value;
		this.valueType = valueType;
	}

}
