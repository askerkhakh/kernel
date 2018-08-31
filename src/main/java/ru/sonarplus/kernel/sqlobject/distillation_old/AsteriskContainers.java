package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;

import java.util.Map;

public class AsteriskContainers extends FieldContainers {

	public AsteriskContainers() {
	}

	@Override
	protected FieldsContainer createFieldsContainerInstance() {
		return new AsteriskContainer();
	}

	public String columnType() {
		return "*";
	}
	
	protected int getTotalLength() {
		int result = 0;
		for (Map.Entry<String, FieldsContainer> entry: items.entrySet()) {
			result += entry.getValue().fieldSpecs.length;
		}
		return result;
	}

	@Override
	public ColumnExprTechInfo[] getFields(String alias) {
		if (!StringUtils.isEmpty(alias)) {
			FieldsContainer	fieldsContainer = findFieldsContainer(alias);	
			Preconditions.checkNotNull(fieldsContainer, "Не удается разрешить колонку %s.%s",
					alias, columnType());
			return fieldsContainer.getFieldSpecs();
		}
		else {
			ColumnExprTechInfo[] result = new ColumnExprTechInfo[getTotalLength()];
			int index = 0;
			for (Map.Entry<String, FieldsContainer> entry: items.entrySet()) {
				for (ColumnExprTechInfo resultItem: entry.getValue().fieldSpecs) {
					result[index] = resultItem;
					index++;
				}
			}
			return result;
		}
	}

}
