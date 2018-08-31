package ru.sonarplus.kernel.dbschema;

public class CodeTypeSpec extends DataTypeSpec {
	public boolean showNull;
	public int posPerPart;
	public int bytesForCode;

	public CodeTypeSpec() {
		super();
	}
	
	public boolean getShowNull() {
		return showNull;
	}
	
	public int getPosPerPart() {
		return posPerPart;
	}

    @Override
    public int getSize() {
        return this.bytesForCode;
    }

	public int getBytesForCode() {
		return bytesForCode;
	}

}
