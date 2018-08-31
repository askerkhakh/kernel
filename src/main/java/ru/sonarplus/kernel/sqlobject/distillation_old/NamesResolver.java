package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/*
 NamesResolver: карта-словарь для разрешения имен полей.
    1. Содержит таблицу вида:
    поле1         = pole1
    [TABLE].поле1 = pole1
    pole1         = pole1
    [TABLE].pole1 = pole1
    ...
    Добавлению дублирующихся имен (в левой части) никак не мешает, но сохраняет их
    в списках дубликатов. При попытке разрешить такое имя будет выдана ошибка о неоднозначном имени.
    2. Для разрешения '*' содержит таблицу вида:
      TABLE = pole1,pole2,pole3...
      ...
    3. Для разрешения идентификатора записи содержит таблицу вида:
      TABLE = pole
      TABLE2 = pole1,pole2
      TABLE3 = pole1,...poleN
 */

public class NamesResolver {
	protected DbcRec dbc;
	public Map<SqlQuery, NamesSpace> nameSpaces = new HashMap<SqlQuery, NamesSpace>();

	public NamesResolver(DbcRec dbc) {
		this.dbc = dbc;
	}

    public class NamesResolverException extends SqlObjectException {
        public NamesResolverException(String message) {
            super(message);
        }
    }

    public static class NameResolver {
        public FromClauseItem fromItem;
        public ColumnExprTechInfo fieldSpec;
        
        public NameResolver(ColumnExprTechInfo fieldSpec, FromClauseItem fromItem) {
        	this.fieldSpec = fieldSpec;
        	this.fromItem = fromItem;
        }
		
	}
	
	public static class NamesSpace {
		public SqlQuery query;
		public Map<String, NameResolver> namesResolving = new HashMap<String, NameResolver>(); 
		public Set<String> duplicates = new HashSet<String>(); 
		public AsteriskContainers asterisks = new AsteriskContainers();
		public RecordIdContainers recordIds = new RecordIdContainers();
		
		public NamesSpace(SqlQuery query) {
			this.query = query;
		}
		
		public NameResolver findResolver(String key) {
			return namesResolving.get(key);
		}
		
		public boolean isExistsResolving(String key) {
			return findResolver(key) != null;
		}
		
		public boolean isDuplicated(String key) {
			return duplicates.contains(key); 
		}
	}
	
	public static SqlQuery queryOrCursorSpec(SqlQuery query) {
		SqlQuery result = query;
		if ((result.getOwner() != null) && (result.getOwner() instanceof CursorSpecification)) {
			result = (CursorSpecification) result.getOwner();
		}
		return result;
	}
	
	public void createNamesSpace(SqlQuery query) {
		SqlQuery result = queryOrCursorSpec(query);
		NamesSpace value = new NamesSpace(query);
		NamesSpace oldValue = nameSpaces.put(result, value);
		if (oldValue != null) {
			nameSpaces.put(result, oldValue);
		}
		
	}
	
	public boolean isExistsNamesSpaceForQuery(SqlQuery query) {
		return nameSpaces.containsKey(query);
	}
	
	public static SqlQuery findTopQuery(SqlObject start) {
		SqlObject item = start;
		while (item != null) {
			item = item.getOwner();
			if (item instanceof SqlQuery) {
				return (SqlQuery) item;
			}
		}
		return null;
	}
	
	protected NamesSpace findNamesSpace(SqlQuery parentQuery) {
		SqlQuery query = parentQuery;
		while ((query != null) && (!isExistsNamesSpaceForQuery(query))) {
			query = findTopQuery(query);
		}
		if (query != null) {
			return nameSpaces.get(query);
		}
		return null;
	}
	
	protected NamesSpace getExistingNamesSpace(SqlQuery parentQuery) {
		NamesSpace result = findNamesSpace(parentQuery);
		Preconditions.checkNotNull(result);
		return result;
	}

	public void addAsterisk(SqlQuery query, String alias, TableSpec tableSpec) {
		getExistingNamesSpace(query).asterisks.add(alias, tableSpec);
	}
	
	public void addAsterisk(SqlQuery query, String alias, ColumnExprTechInfo[] fields) {
		getExistingNamesSpace(query).asterisks.add(alias, fields);	
	}
	
	public void addRecordId(SqlQuery query, String alias, TableSpec tableSpec) {
		getExistingNamesSpace(query).recordIds.add(alias, tableSpec);
	}
	
	public void addResolving(NamesSpace namesSpace, String key, ColumnExprTechInfo fieldSpec,
			FromClauseItem fromItem) {
  	    NameResolver oldValue = namesSpace.namesResolving.put(key, new NameResolver (fieldSpec, fromItem));
  	    if (oldValue != null) {
  	    	namesSpace.namesResolving.put(key, oldValue);
  	    	namesSpace.duplicates.add(key);
  	    }
	}
	
	public void addResolving(SqlQuery query, String key, ColumnExprTechInfo fieldSpec, FromClauseItem fromItem) {
		addResolving(getExistingNamesSpace(query), key, fieldSpec, fromItem);
	}
	
	public void addResolving(SqlQuery query, String[] keys, ColumnExprTechInfo fieldSpec, FromClauseItem fromItem) {
		NamesSpace space = getExistingNamesSpace(query);
		for (String item: keys) {
			addResolving(space, item, fieldSpec, fromItem);
		}
	}
	
	public static String sourceName(String alias, SqlQuery query) {
		String sourceName = alias;
		if (StringUtils.isEmpty(sourceName) && (query instanceof CursorSpecification || query instanceof Select)) {
			String tableAlias = SqlObjectUtils.getRequestTableAlias(query);
			if (!StringUtils.isEmpty(tableAlias))
				sourceName = tableAlias;
			else
				sourceName = SqlObjectUtils.getRequestTableName(query);
		}
		else if (query instanceof DataChangeSqlQuery)
			sourceName = ((DataChangeSqlQuery)query).table;

		if (!StringUtils.isEmpty(sourceName)) {
			return String.format("источника записей \"%s\"", sourceName);
		}
		else {
			return "";
		}
	}
	
	public ColumnExprTechInfo resolveName(SqlQuery query, String key)
            throws NamesResolverException {
		NamesSpace space = findNamesSpace(query);
		if (space != null) {
            if (space.isDuplicated(key))
                throw new NamesResolverException(String.format("Невозможно однозначно определить колонку \"%s\" %s", key, sourceName(QualifiedName.stringToQualifiedName(key).alias, query)));
			NameResolver nameResolver = space.findResolver(key);
			if (nameResolver != null) {
				return nameResolver.fieldSpec;
			}
		}
		return null;
		
	}
	
	public ColumnExprTechInfo[] asteriskFields(SqlQuery query, String alias) {
		return getExistingNamesSpace(query).asterisks.getFields(alias);
	}
	
	public ColumnExprTechInfo[] recordIdFields(SqlQuery query, String alias) {
		return getExistingNamesSpace(query).recordIds.getFields(alias);
	}
	
	public String[] recordIdColumns(SqlQuery query, String alias) {
		ColumnExprTechInfo[] items = recordIdFields(query, alias);
		String[] result = new String[items.length];
		for (int i = 0; i < items.length; i++) {
			result[i] = items[i].nativeFieldName;
		}
		return result;
	}

	public FieldTypeId[] recordIdTypes(SqlQuery query, String alias) {
		ColumnExprTechInfo[] items = recordIdFields(query, alias);
		FieldTypeId[] result = new FieldTypeId[items.length];
		for (int i = 0; i < items.length; i++) {
			result[i] = items[i].fieldTypeId;
		}
		return result;
	}
	
	
	
	
}
