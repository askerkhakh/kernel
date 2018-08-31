package ru.sonarplus.kernel.dbschema;

import com.google.common.base.Preconditions;

public class NumberTypeSpec extends DataTypeSpec {
	public boolean showNull;
	public boolean showLeadingNull;
	public boolean summable;
	public boolean hasThousandSeparator;
	public int precision;

	public NumberTypeSpec() {
		super();
	}
	
	public boolean getShowNull() {
		return showNull;
	}
	
	public boolean getShowLeadingNull() {
		return showLeadingNull;
	}

	public boolean getSummable() {
		return summable;
	}

	public boolean getHasThousandSeparator() {
		return hasThousandSeparator;
	}
	
	public int getPrecision() {
		return precision;
	}

    @Override
    public int getSize() {
        switch (this.fieldTypeId) {
            case tid_BYTE:
            case tid_SMALLINT:
            case tid_WORD:
            case tid_INTEGER:
            case tid_LARGEINT:
            case tid_FLOAT:
                return super.getSize();
            default:
                Preconditions.checkState(false, getClass().getSimpleName() + ": " + String.valueOf(this.fieldTypeId));
        }
        return 0;
    }
}
