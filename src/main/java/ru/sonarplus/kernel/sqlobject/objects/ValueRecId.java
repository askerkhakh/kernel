package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.RecIdValueWrapper;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

public class ValueRecId extends Value {

	private final RecIdValueWrapper recId;

	public ValueRecId(RecIdValueWrapper recId) {
		super();
		this.recId = recId;
	}

	public ValueRecId(SqlObject owner, RecIdValueWrapper recId) {
		super(owner);
		this.recId = recId;
	}

	public RecIdValueWrapper getValue() {
		return recId;
	}

	public RecIdValueWrapper getRecId() {
	    return recId;
    }


    // FIXME такое количество перекрытых и ничего не делающих методов говорит о плохом дизайне. Требуется рефакторинг.
    @Override
    public void setValue(Object value) throws ValuesSupport.ValueException {
        throw new AssertionError();
    }

    @Override
    public FieldTypeId getValueType() {
        throw new AssertionError();
    }

    @Override
    public void setValueType(FieldTypeId value) throws ValuesSupport.ValueException {
        throw new AssertionError();
    }

}
