package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;

public class DistillationParamsIn {
	public FieldTypeId valueType;
	public boolean isNeedCloneAndRowRestrict;

	public DistillationParamsIn(FieldTypeId type) {
		valueType = type;
		isNeedCloneAndRowRestrict = false;
	}
	
	public DistillationParamsIn(boolean isNeedCloneAndRowRestrict) {
		this(FieldTypeId.tid_UNKNOWN);
		this.isNeedCloneAndRowRestrict = isNeedCloneAndRowRestrict;
	}

}
