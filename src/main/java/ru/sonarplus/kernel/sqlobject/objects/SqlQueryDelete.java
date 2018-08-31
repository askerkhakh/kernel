package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class SqlQueryDelete extends DataChangeSqlQuery {

    protected Conditions where = null;

	public SqlQueryDelete() { super(); }

	public SqlQueryDelete(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
	
	public Conditions findWhere() {
		return where;
	}

	public void setWhere(Conditions value)
            throws SqlObjectException {
	    if (where != value) {
            unassignItem(where);
            where = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        SqlQueryDelete targetInstance = (SqlQueryDelete) target;
        targetInstance.where = where == null ? null : targetInstance.setOwner((Conditions) where.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item instanceof Conditions) {
            if (this.where == null)
                this.where = (Conditions) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.where) {
            unassignItem(this.where); this.where = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.where)
            this.where = (Conditions) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(where); where = null;
    }


    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{params, where};
    }

}
