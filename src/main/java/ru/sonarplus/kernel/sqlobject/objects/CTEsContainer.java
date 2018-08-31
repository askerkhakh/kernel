package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class CTEsContainer extends SqlObjectsArray {

	public CTEsContainer() { super(); }

	public CTEsContainer(SqlObject owner) throws SqlObjectException {
		super(owner);
	}

	@Override
    protected Class getItemClass () { return CommonTableExpression.class; }

}
