package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class TupleExpressions extends SqlObjectsArray {

    public TupleExpressions() {super();}

    public TupleExpressions(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}

	@Override
    protected Class getItemClass () { return ColumnExpression.class; }
}
