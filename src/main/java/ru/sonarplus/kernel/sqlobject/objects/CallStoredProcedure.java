package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class CallStoredProcedure extends SqlQuery {
    protected TupleExpressions tuple = null;

	public String spName;

    public CallStoredProcedure() { super(); }
    public CallStoredProcedure(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

    public TupleExpressions getTuple() {
        return Preconditions.checkNotNull(tuple);
    }

    public TupleExpressions findTuple() {
        return tuple;
    }

    public TupleExpressions newTuple() {
        if (tuple == null)
            tuple = setOwner(new TupleExpressions());
        return tuple;
    }

    public void setTuple(TupleExpressions value)
            throws SqlObjectException {
	    unassignItem(tuple);
	    tuple = assignItem(value);
    }
	
    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        ((CallStoredProcedure)target).tuple = this.tuple == null? null : target.setOwner((TupleExpressions)this.tuple.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {
        if (item.getClass() == TupleExpressions.class && this.tuple == null)
            this.tuple = assignItem((TupleExpressions)item);
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
        if (this.tuple != null)
            this.tuple.destroyItems();
        super.internalDestroyItems();
    }

    @Override
    public SqlObject[] getSubItems () { return new SqlObject[]{this.tuple}; }

}
