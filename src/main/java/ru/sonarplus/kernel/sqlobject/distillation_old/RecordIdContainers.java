package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;

import java.util.Map;

public class RecordIdContainers extends FieldContainers {

	public RecordIdContainers() {
	}

	@Override
	protected FieldsContainer createFieldsContainerInstance() {
		return new RecordIdContainer();
	}

	@Override
	public String columnType() {
		return "recid";
	}
	
	@Override
	public ColumnExprTechInfo[] getFields(String alias) {
		if (!StringUtils.isEmpty(alias)) {
			return super.getFields(alias);
		}
		else {
			for (Map.Entry<String, FieldsContainer> entry: items.entrySet()) {
				return entry.getValue().fieldSpecs;
			}
		}
		Preconditions.checkArgument(false,"Не найден элемент с алиасом "+alias);
		return null;
	}
	

}
