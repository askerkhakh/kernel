package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class SqlQuery extends SqlObject {

    protected QueryParams params = null;
    public String hint;
	public SqlQueryTechInfo techInfo = new SqlQueryTechInfo();

	public SqlQuery() { super(); }
	public SqlQuery(SqlObject owner)
            throws SqlObjectException { super(owner); }

	public QueryParams getParams() {
		return params;
	}

	public QueryParams newParams() {
	    if (this.params == null)
            this.params = setOwner(new QueryParams());
        return this.params;
	}

	public void setParams(QueryParams value)
            throws SqlObjectException {
	    if (this.params != value) {
            unassignItem(params);
            params = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        ((SqlQuery) target).params = params == null ? null : target.setOwner((QueryParams) params.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item instanceof ColumnExpression) {
            if (this.params == null)
                this.params = (QueryParams) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.params) {
            unassignItem(this.params); this.params = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.params)
            this.params = (QueryParams) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(params); params = null;
    }

}
