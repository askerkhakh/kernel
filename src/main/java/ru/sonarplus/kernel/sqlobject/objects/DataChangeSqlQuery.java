package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataChangeSqlQuery extends SqlQuery {

	protected TupleExpressions returning = null;

	public static final String VARIABLES_DELIMITER = ",";
	public String hint;
	
	public String schema;
	public String table;
	// TODO Непонятно, нужны ли на самом деле эти DataChangeSqlQuery.intoVariables
	public List<String> intoVariables = new ArrayList<String>();

	public DataChangeSqlQuery() { super(); }

	public DataChangeSqlQuery(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

	public String getIntoVariables() {
		return String.join(VARIABLES_DELIMITER, intoVariables);
	}
	
	public void setIntoVariables(String value) {
		intoVariables = Arrays.asList(value.split(VARIABLES_DELIMITER));
	}
	
    public TupleExpressions getReturning() { return returning; }

    public TupleExpressions newReturning() {
	    if (returning == null)
	        returning = setOwner(new TupleExpressions());
	    return returning;
    }

    public void setReturning(TupleExpressions value)
            throws SqlObjectException {
	    if (returning != value) {
	        unassignItem(returning);
	        returning = assignItem(value);
        }
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        DataChangeSqlQuery targetQuery = (DataChangeSqlQuery) target;
        if (intoVariables != null)
            targetQuery.intoVariables.addAll(intoVariables);

        targetQuery.returning = returning == null ? null : target.setOwner((TupleExpressions) returning.clone());
    }

    @Override
    protected void internalInsertItem(SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == TupleExpressions.class) {
            if (this.returning == null)
                returning = (TupleExpressions) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.returning) {
            unassignItem(this.returning); this.returning = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace(SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == returning)
            returning = (TupleExpressions) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(returning); returning = null;
    }


}
