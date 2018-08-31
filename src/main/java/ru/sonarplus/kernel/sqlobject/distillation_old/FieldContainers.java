package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;

import java.util.HashMap;
import java.util.Map;

public abstract class FieldContainers {
	Map<String, FieldsContainer> items = new HashMap<String, FieldsContainer>();

	public FieldContainers() {
	}
	
	public FieldsContainer findFieldsContainer(String alias) {
		return items.get(alias);
	}
	
	protected abstract FieldsContainer createFieldsContainerInstance();
    public abstract String columnType();

	
	public FieldsContainer createFieldsContainer(String alias) {
		FieldsContainer container = createFieldsContainerInstance();
		FieldsContainer prevContainer = items.put(alias, container);
		if (prevContainer != null) {
			items.put(alias, prevContainer);
			Preconditions.checkArgument(false, "Элемент с именем %s уже есть в контейнере", alias);
		}
		return container;
	}
	
	public void add(String alias, TableSpec tableSpec) {
		createFieldsContainer(alias).tableSpec = tableSpec;
	}
	
	public void add(String alias, ColumnExprTechInfo[] value) {
		createFieldsContainer(alias).setFieldSpecs(value);
	}
	
	public  ColumnExprTechInfo[] getFields(String alias) {
		FieldsContainer container = findFieldsContainer(alias); 
		Preconditions.checkNotNull(container, "Не удается разрешить колонку %s.%s",
				alias, columnType());
		return container.getFieldSpecs();
		
	}
	
	
	

}
