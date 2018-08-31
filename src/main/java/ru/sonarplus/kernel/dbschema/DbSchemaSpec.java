package ru.sonarplus.kernel.dbschema;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DbSchemaSpec extends DbdItemWithAttributes{
	protected List<DomainSpec> domainsList = new ArrayList<>();
	/* Причина комментирования описана в методе findDomainSpecByName
	protected Map<String, DomainSpec> domainsMap = new HashMap<String, DomainSpec>();
	*/
	protected List<TableSpec> tablesList = new ArrayList<>();
	protected Map<String, TableSpec> tablesMap = new HashMap<>();
	
	protected Map<String, List<ConstraintSpec>> detailsMap = new HashMap<>();
 
	public DbSchemaSpec() {
		// TODO Auto-generated constructor stub
	}
	
	public int getDomainSpecCount() {
		return domainsList.size();
	}
	
	public DomainSpec getDomainSpec(int index) {
		return domainsList.get(index);
	}

	/* 
	Возможность получения домена по его имени (findDomainSpecByName) была закомментирована.
	Причина: при загрузке из нескольких описателей в одну схему не гарантируется уникальность имени домена.
	  Необходимость данной функции и, соответственно, поиск решения на данный момент кажется избыточным.

	public DomainSpec findDomainSpecByName(String name) {		
		return domainsMap.get(name);
	}
	*/
	
	public int getTableSpecCount() {
		return tablesList.size();
	}
	
	public TableSpec getTableSpec(int index) {
		return tablesList.get(index);
	}
	
    @Nullable
	public TableSpec findTableSpec(String name) {
		return tablesMap.get(name);
	}

	public TableSpec getTableSpecByName(String tableName) {
	    return requireNonNull(findTableSpec(tableName));
    }
	
	public void addTableSpec(TableSpec tableSpec) {
		String tableName = tableSpec.getName();
		Preconditions.checkArgument(!tablesMap.containsKey(tableName), 
				"Таблица '%1s' уже загружена в описатель", tableName);
		tablesList.add(tableSpec);
		tablesMap.put(tableName, tableSpec);
	}
	
	public void addDomainSpec(DomainSpec domainSpec) {
		domainsList.add(domainSpec);
		/* Причина комментирования описана в методе findDomainSpecByName		
		// На данный момент (02/06/2017) поддержана работа с неименованными доменами - их добавлять не будем
		if (domainSpec.getDomainName() != null){
			domainsMap.put(domainSpec.getDomainName(), domainSpec);
		}
		*/
	}
	
	public void buildDetailsMap() {
		detailsMap.clear();
		for (TableSpec item: tablesList) {
			if (item.getPrimaryKey() != null) {
				detailsMap.put(item.getName(), new ArrayList<>());
			}
		}
		for (TableSpec item: tablesList) {
			for (ConstraintSpec spec: item.constraintItems) {
				if (spec instanceof ForeignKeyConstraintSpec) {
					ForeignKeyConstraintSpec foreignSpec = (ForeignKeyConstraintSpec) spec;
					String targetName = foreignSpec.targetConstraint.tableSpec.getName();
					Preconditions.checkNotNull(detailsMap.get(targetName)).add(foreignSpec);
				}
			}
		}
	}
	
    @Nullable
	public List<ConstraintSpec> getTableDetails(String tableName) {
		List<ConstraintSpec> result = detailsMap.get(tableName);
		if ((result != null) && (result.size() == 0)) {
			result = null;
		}
		return result;
	}
	

}

/*
05.06.2017 11:00 А.В.Шаров
 P* Код задачи 43210. Поддержана загрузка пользовательского метаописателя.
*/