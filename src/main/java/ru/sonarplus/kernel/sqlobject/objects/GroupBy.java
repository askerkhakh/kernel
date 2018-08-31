package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class GroupBy extends SqlObject {

    protected TupleExpressions tuple = null;
    protected Conditions having = null;

	public GroupBy()  { super(); }

	public GroupBy(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
	
	public GroupBy addGroup(ColumnExpression expr)
            throws SqlObjectException {
		if (expr != null) {
			getTupleItems().insertItem(expr);
		}
		return this;
	}

	public TupleExpressions findTuple() { return tuple; }

	public TupleExpressions getTupleItems() {
	    if (tuple == null)
	        tuple = setOwner(new TupleExpressions());
	    return tuple;
	}

    public void setTupleItems(TupleExpressions value)
            throws SqlObjectException {
	    if (this.tuple != value) {
            unassignItem(this.tuple);
            this.tuple = assignItem(value);
        }
    }

	public Conditions getHaving() { return having; }

	public Conditions newHaving() {
	    if (having == null)
	        having = setOwner(new Conditions());
		return having;
	}

	public void setHaving(Conditions value)
            throws SqlObjectException {
	    if (this.having != value) {
	        unassignItem(this.having);
	        this.having = assignItem(value);
        }
	}

	public final boolean isHasChilds() {
	    return (tuple != null || having != null);
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        ((GroupBy) target).tuple = tuple == null ? null : target.setOwner((TupleExpressions) tuple.clone());
        ((GroupBy) target).having = having == null ? null : target.setOwner((Conditions) having.clone());
    }

    @Override
    protected void internalInsertItem(SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == TupleExpressions.class) {
            if (this.tuple == null)
                this.tuple = (TupleExpressions) item;
            else
                super.internalInsertItem(item);
        }
        else if (item instanceof Conditions) {
            if (this.having == null)
                this.having = (Conditions) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.tuple) {
            unassignItem(this.tuple); this.tuple = null;
        }
        else if (item == this.having) {
            unassignItem(this.having); this.having = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.tuple)
            this.tuple = (TupleExpressions) with;
        else if (what == this.having)
            this.having = (Conditions) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(tuple); tuple = null;
        unassignItem(having); having = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{tuple, having};
    }

}
