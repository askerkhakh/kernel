package ru.sonarplus.kernel.dbschema;

public class DomainSpec extends DbdItemWithAttributes{
	public DataTypeSpec dataTypeSpec;
	public String name;
	public String description;

	
	public DomainSpec() {
		// TODO Auto-generated constructor stub
	}
	
	public DataTypeSpec getDataTypeSpec() {
		return dataTypeSpec;
	}
	
	public String getDomainName() {
		return name;
	}

	public String getDomainDescription() {
		return description;
	}
}
