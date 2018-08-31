package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class OrderByItem extends SqlObject {

    public enum OrderDirection {
        ASC, DESC
    }

    public enum NullOrdering {
        NONE, NULLS_FIRST, NULLS_LAST
    }

    protected ColumnExpression expr = null;

	public OrderDirection direction = OrderDirection.ASC;
	public NullOrdering nullOrdering = NullOrdering.NONE;

    public OrderByItem() { super(); }

    public OrderByItem(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }
	public ColumnExpression getExpr() {
		return expr;
	}

	public void setExpr(ColumnExpression value)
            throws SqlObjectException {
		if (expr != value) {
		    unassignItem(expr);
		    expr = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        ((OrderByItem) target).expr = expr == null ? null : target.setOwner((ColumnExpression) expr.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item instanceof ColumnExpression) {
            if (this.expr == null)
                this.expr = (ColumnExpression) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.expr) {
            unassignItem(this.expr); this.expr = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.expr)
            this.expr = (ColumnExpression) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(expr); expr = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{expr};
    }
}
