package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.IndexSpec;
import ru.sonarplus.kernel.dbschema.IndexType;
import ru.sonarplus.kernel.dbschema.PrimaryKeyConstraintSpec;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;

public class RecordIdContainer extends FieldsContainer {

	public RecordIdContainer() {
	}

	@Override
	public FieldSpec[] getIFieldSpecs() {
		PrimaryKeyConstraintSpec primarySpec = tableSpec.getPrimaryKey();
		FieldSpec[] result;
		if (primarySpec != null) {
			result = new FieldSpec[primarySpec.getItemsCount()];
			for (int i = 0; i < result.length; i++) {
				result[i] = primarySpec.getItem(i);
			}
			return result;
		}
		for (IndexSpec indexSpec: tableSpec.indexItems) {
			if ((indexSpec.indexType == IndexType.UNIQUE) && (indexSpec.getItemsCount() == 1)) {
				FieldSpec uniqueSpec = indexSpec.getItem(0).fieldSpec;	
				if (uniqueSpec != null) {
					return new FieldSpec[]{uniqueSpec};
				}
			}
		}
		Preconditions.checkArgument(false, "Не найден уникальный ключ");
		return null;
	}
	
	@Override
	public void setFieldSpecs(ColumnExprTechInfo[] fieldSpecs) {
		Preconditions.checkArgument(false,"Указание списка полей запрещено для "+this.getClass().getSimpleName());
	}
	
}
