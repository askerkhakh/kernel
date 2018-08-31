package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

//выбранная в SELECT колонка
public class SelectedColumn extends SqlObject {

    protected ColumnExpression expr = null;
	public String alias;

    public SelectedColumn() { super(); }
    public SelectedColumn(ColumnExpression expression, String alias)
            throws SqlObjectException {
        this();
        setExpression(expression);
        this.alias = alias;
    }
    public SelectedColumn(ColumnExpression expression)
            throws SqlObjectException { this(expression, ""); }

    public SelectedColumn(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

	public SelectedColumn(SqlObject owner, ColumnExpression expression, String alias)
            throws SqlObjectException {
		this(owner);
		setExpression(expression);
		this.alias = alias;
	}

    public SelectedColumn(SqlObject owner, ColumnExpression expression)
            throws SqlObjectException {
        this(owner, expression, "");
    }

	public ColumnExpression getColExpr() {
		return expr;
	}

	public SelectedColumn setExpression(ColumnExpression value)
            throws SqlObjectException {
        if (expr !=  value) {
            unassignItem(expr);
            expr = assignItem(value);
        }
		return this;
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        SelectedColumn targetInstance = (SelectedColumn) target;
        targetInstance.expr = expr == null ? null : target.setOwner((ColumnExpression) expr.clone());
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
