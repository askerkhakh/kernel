package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class ColumnExpression extends SqlObject {
    public String id = null;
	public ColumnExprTechInfo distTechInfo;

	public ColumnExpression() { super(); }
    public ColumnExpression(SqlObject owner)
            throws SqlObjectException { super(owner); }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {

        super.internalClone(target);
        if (distTechInfo != null)
            ((ColumnExpression)target).distTechInfo = distTechInfo.clone();
    }
}
