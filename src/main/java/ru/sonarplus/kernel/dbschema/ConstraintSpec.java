package ru.sonarplus.kernel.dbschema;

public class ConstraintSpec extends DbdItemWithAttributes{
	public TableSpec tableSpec;
	public String name;
	public ConstraintType constraintType;

	public ConstraintSpec() {
		// TODO Auto-generated constructor stub
	}
	
	public TableSpec getTableSpec() {
		return tableSpec;
	}
	
	public String getName() {
		return name;
	}
	
	public ConstraintType getConstraintType() {
		return constraintType;
	}

}
