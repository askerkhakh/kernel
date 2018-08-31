package ru.sonarplus.kernel.dbschema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TableSpec extends DbdItemWithAttributes {
	public List<FieldSpec> items = new ArrayList<FieldSpec>();
	protected Map<String, FieldSpec> itemsMap = new HashMap<String, FieldSpec>();
	
	public List<IndexSpec> indexItems = new ArrayList<IndexSpec>();
	
	public List<ConstraintSpec> constraintItems = new ArrayList<ConstraintSpec>(); 
	public PrimaryKeyConstraintSpec primaryKey; 
	
	public String name;
	public String title;
	public boolean canAdd;
	public boolean canDelete;
	public boolean canEdit;
	public String temporalMode;
	protected DbSchemaSpec dbSchemaSpec;

	public TableSpec(DbSchemaSpec dbSchemaSpec) {
		this.dbSchemaSpec = dbSchemaSpec;
	}
	
	public DbSchemaSpec getDBSchemaSpec() {
		return this.dbSchemaSpec;
	}

	public Iterable<FieldSpec> getFields() {
		return items;
	}
	
	public int getFieldSpecCount() {
		return items.size();
	}
	
	public FieldSpec getFieldSpec(int index) {
		return items.get(index);
	}
	
	public FieldSpec findFieldSpecByName(String name){
		return itemsMap.get(name);
	}
	
	public int getIndexSpecCount() {
		return indexItems.size();
	}
	
	public IndexSpec getIndexSpec(int index) {
		return indexItems.get(index);
	}
	
	public int getConstraintSpecCount() {
		return constraintItems.size();
	}
	
	public ConstraintSpec getConstraintSpec(int index) {
		return constraintItems.get(index);
	}
	
	public PrimaryKeyConstraintSpec getPrimaryKey() {
		return primaryKey;
	}
	
	
	
	public String getName() {
		return name;
	}
	
	public String getTitle() {
		return title;
	}
	
	public boolean getCanAdd() {
		return canAdd;
	}
	
	public boolean getCanDelete() {
		return canDelete;
	}

	public boolean getCanEdit() {
		return canEdit;
	}
	
	public void buildMap() {
		for (FieldSpec spec: items) {
			itemsMap.put(spec.getFieldName(), spec);
		}
	}	
}
