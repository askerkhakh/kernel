package ru.sonarplus.kernel.dbschema;

public class DataTypeSpec {
	public int displayWidth;
	public Alignment alignment;
	public FieldTypeId fieldTypeId;

	public DataTypeSpec() {
	}
	
	public FieldTypeId getFieldTypeId() {
		return fieldTypeId;
	}

    public int getSize() {
        return SDataTypes.defaultTypeIDNatSize(this.fieldTypeId);
	}
	
	public int getDisplayWidth() {
		return displayWidth;
	}
	
	public Alignment getAlignment() {
		return alignment;
	}

}
