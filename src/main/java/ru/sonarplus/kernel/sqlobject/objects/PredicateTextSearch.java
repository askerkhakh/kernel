package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateTextSearch extends PredicateComparisonCommon {

	public PredicateTextSearch () { super(); }

	public PredicateTextSearch(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

	public ColumnExpression getExpr() {
		return getLeft();
	}
    public void setExpr(ColumnExpression value)
            throws SqlObjectException {
        setLeft(value);
    }
	
	public ColumnExpression getTemplate() {
		return getRight();
	}
    public void setTemplate(ColumnExpression value)
            throws SqlObjectException {
        setRight(value);
    }


}
