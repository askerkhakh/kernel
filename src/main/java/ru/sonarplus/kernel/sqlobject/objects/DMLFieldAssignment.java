package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class DMLFieldAssignment extends SqlObject {

	protected QualifiedField field = null;
	protected ColumnExpression expr = null;

    public DMLFieldAssignment() { super(); }

	public DMLFieldAssignment(SqlObject owner) {
		super(owner);
	}
	
	public QualifiedField getField() {
		return field;
	}

	public void setField(QualifiedField value)
            throws SqlObjectException {
		if (field != value) {
		    unassignItem(field);
		    field = assignItem(value);
        }
	}

	public ColumnExpression getExpr() {
		return expr;
	}

	public void setExpr(ColumnExpression value)
            throws SqlObjectException {
		if (expr != value){
		    unassignItem(expr);
		    expr = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        DMLFieldAssignment targetAssignment = (DMLFieldAssignment) target;
        targetAssignment.field = field == null ? null : target.setOwner((QualifiedField) field.clone());
        targetAssignment.expr = expr == null ? null : target.setOwner((ColumnExpression) expr.clone());
    }

    @Override
    protected void internalInsertItem(SqlObject item)
            throws SqlObjectException {
        if (item instanceof QualifiedField) {
            if (this.field == null)
                this.field = (QualifiedField) item;
            else if (this.expr == null)
                this.expr = (QualifiedField) item;
            else
                super.internalInsertItem(item);
        }
        else if (item instanceof ColumnExpression) {
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
        if (item == this.field) {
            unassignItem(this.field); this.field = null;
        }
        else if (item == this.expr) {
            unassignItem(this.expr); this.expr = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.field)
            this.field = (QualifiedField) with;
        else if (what == this.expr)
            this.expr = (ColumnExpression) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(field); field = null;
        unassignItem(expr); expr = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{field, expr};
    }

}
