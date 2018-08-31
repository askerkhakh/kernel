package ru.sonarplus.kernel.dbschema;

public class IndexItemSpec {
	public FieldSpec fieldSpec;
	public String functionDesc;
	public IndexOrder indexOrder;

	public IndexItemSpec() {
		// TODO Auto-generated constructor stub
	}
    public FieldSpec getFieldSpec() {
    	return fieldSpec;
    }
    
    public String getFunctionDesc() {
    	return functionDesc;
    }
    
    public IndexOrder getOrder() {
    	return indexOrder;
    }

}
