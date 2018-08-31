package ru.sonarplus.kernel.dbschema;

public class FieldSpec extends DomainSpec {
	public String name;
	public String latinName;
	public String description;
	public TableSpec tableSpec;
	public boolean canInput;
	public boolean canEdit;
	public boolean showInGrid;
	public boolean showInDetails;
	public boolean isMean;
	public boolean autoAssignable;
	public boolean isNotNull;
	public DomainSpec domainSpec;

	public FieldSpec(TableSpec tableSpec) {
		this.tableSpec = tableSpec;
	}
	
	public TableSpec getTableSpec() {
		return tableSpec;
	}
	
	public String getFieldName() {
		return name;
	}
	
	public String getLatinName() {
		return latinName;
	}
	
	public String getFieldDescription() {
		return description;
	}
	
    public boolean getCanInput() {
    	return canInput;
    }
    
    public boolean getCanEdit() {
    	return canEdit;
    }

    public boolean getShowInGrid() {
    	return showInGrid;
    }
    
    public boolean getShowInDetails() {
    	return showInDetails;
    }
    
    public boolean getIsMean() {
    	return isMean;
    }

    @Deprecated
    public boolean getAutoAssignable() {
    	return autoAssignable;
    }
    
    public boolean getIsNotNull() {
    	return isNotNull;
    }
    
    @Override
	public DataTypeSpec getDataTypeSpec() {
		return domainSpec.getDataTypeSpec();
	}
	
    @Override
	public String getDomainName() {
		return domainSpec.getDomainName();
	}

    @Override
	public String getDomainDescription() {
		return domainSpec.getDomainDescription();
	}
    

}
