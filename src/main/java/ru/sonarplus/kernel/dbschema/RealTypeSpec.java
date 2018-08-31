package ru.sonarplus.kernel.dbschema;

public class RealTypeSpec extends NumberTypeSpec {
	public int scale;

	public RealTypeSpec() {
	}
	
	public int getScale() {
		return scale;
	}

}
