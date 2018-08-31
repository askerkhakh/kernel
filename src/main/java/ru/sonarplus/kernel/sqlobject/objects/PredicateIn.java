package ru.sonarplus.kernel.sqlobject.objects;

//... IN(..)
// Tuple - кортеж, вхождение КОТОРОГО (или В КОТОРЫЙ) проверяем

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateIn extends Predicate {

	protected TupleExpressions tuple = null;

    public PredicateIn() { super(); }

	public PredicateIn(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}

	public TupleExpressions getTuple() {
		return tuple;
	}
	public TupleExpressions newTuple() {
        if (tuple == null)
            tuple = setOwner(new TupleExpressions());
        return tuple;
    }

	public void setTuple(TupleExpressions value)
            throws SqlObjectException {
		if (tuple != value) {
		    unassignItem(tuple);
		    tuple = assignItem(value);
        }
	}

	public PredicateIn tupleAdd(ColumnExpression value)
            throws SqlObjectException {
		if (value != null)
			newTuple().insertItem(value);
		return this;
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        PredicateIn targetInstance = (PredicateIn) target;
        targetInstance.tuple = tuple == null ? null : target.setOwner((TupleExpressions) tuple.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == TupleExpressions.class) {
            if (this.tuple == null)
                this.tuple = (TupleExpressions) item;
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
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.tuple)
            this.tuple = (TupleExpressions) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(tuple); tuple = null;
    }

}
