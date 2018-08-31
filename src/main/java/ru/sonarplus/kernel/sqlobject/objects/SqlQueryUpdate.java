package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import static com.google.common.base.Verify.verifyNotNull;

public class SqlQueryUpdate extends DataChangeSqlQuery {

    protected DMLFieldsAssignments assignments = null;
    protected Conditions where = null;

	public SqlQueryUpdate() { super(); }

	public SqlQueryUpdate(SqlObject owner)
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

	public void set(QualifiedField field, ColumnExpression expr)
            throws SqlObjectException {
		Preconditions.checkNotNull(field,
				"Не задано поле для запроса Update");
		Preconditions.checkNotNull(expr,
				"Не задано выражение для поля в запросе Update");
		DMLFieldAssignment result = new DMLFieldAssignment(
				newAssignments());
		result.setField(field);
		result.setExpr(expr);
	}

	public DMLFieldsAssignments getAssignments() {
	    return verifyNotNull(assignments);
	}

    public DMLFieldsAssignments newAssignments() {
	    if (assignments == null)
	        assignments = setOwner(new DMLFieldsAssignments());
	    return assignments;
    }

    public void setAssignments(DMLFieldsAssignments value)
            throws SqlObjectException {
        if (assignments != value) {
            unassignItem(assignments);
            assignments = assignItem(value);
        }
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        SqlQueryUpdate targetInstance = (SqlQueryUpdate) target;
        targetInstance.assignments = assignments == null ? null : targetInstance.setOwner((DMLFieldsAssignments) assignments.clone());
        targetInstance.where = where == null ? null : targetInstance.setOwner((Conditions) where.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() ==DMLFieldsAssignments.class) {
            if (this.assignments == null)
                this.assignments = (DMLFieldsAssignments) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == Conditions.class) {
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
        if (item == this.assignments) {
            unassignItem(this.assignments); this.assignments = null;
        }
        else if (item == this.where) {
            unassignItem(this.where); this.where = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.assignments)
            this.assignments = (DMLFieldsAssignments) with;
        else if (what == this.where)
            this.where = (Conditions) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(assignments); assignments = null;
        unassignItem(where); where = null;
    }

    public SqlObject[] getSubItems () {
        return new SqlObject[]{params, assignments, where};
    }

}
