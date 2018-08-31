package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldSpec;

public class AsteriskContainer extends FieldsContainer {

	public AsteriskContainer() {

	}

	@Override
	public FieldSpec[] getIFieldSpecs() {
		Preconditions.checkNotNull(tableSpec);
		FieldSpec[] result = new FieldSpec[tableSpec.getFieldSpecCount()];
		for (int i = 0; i < result.length; i++) {
			result[i] = tableSpec.getFieldSpec(i);
		}
		return result;
	}
	
}
