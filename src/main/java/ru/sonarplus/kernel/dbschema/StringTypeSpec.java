package ru.sonarplus.kernel.dbschema;

public class StringTypeSpec extends DataTypeSpec {
	public boolean isCaseSensitive;
	public int charLength;

	public StringTypeSpec() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public boolean getIsCaseSensitive() {
		return isCaseSensitive;
	}

    @Override
    public int getSize() {
        return this.charLength;
    }

	public int getCharLength() {
		return charLength;
	}

}
