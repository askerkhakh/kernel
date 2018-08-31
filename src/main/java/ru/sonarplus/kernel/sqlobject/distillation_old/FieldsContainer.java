package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;

public abstract class FieldsContainer {
	public TableSpec tableSpec;
	protected ColumnExprTechInfo[] fieldSpecs;

	public FieldsContainer() {
	}
	
	public void setFieldSpecs(ColumnExprTechInfo[] fieldSpecs) {
		this.fieldSpecs = fieldSpecs;
	}
	
	public abstract FieldSpec[] getIFieldSpecs();
	
	public ColumnExprTechInfo[] getFieldSpecs() {
		if (fieldSpecs == null) {
			Preconditions.checkNotNull(tableSpec);
			FieldSpec[] tableFieldSpecs = getIFieldSpecs();
			fieldSpecs = new ColumnExprTechInfo[tableFieldSpecs.length];
			for (int i = 0; i < tableFieldSpecs.length; i++) {
				fieldSpecs[i] = ColumnExprTechInfo.createTechInfoByFieldSpec(tableFieldSpecs[i]);
			}
		}
		return fieldSpecs;
	}

}
