package ru.sonarplus.kernel.dbschema;

public class RecordIdTypeSpec extends DataTypeSpec {
	public int bytesForIdentifier;
	public int bitsForNetAddr;

	public RecordIdTypeSpec() {
		super();
	}
	
	public int getBytesForIdentifier() {
		return bytesForIdentifier;
	}
	
	public int getBitsForNetAddr() {
		return bitsForNetAddr;
	}

}
