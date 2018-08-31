package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import static com.google.common.base.Verify.verifyNotNull;

public class SqlQueryInsert extends DataChangeSqlQuery {

    protected Select select = null;
    protected DMLFieldsAssignments assignments = null;

	public SqlQueryInsert() { super(); }

	public SqlQueryInsert(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
	
	public Select findSelect() { return select; }

	public Select getSelect() { return Preconditions.checkNotNull(select); }

    public void setSelect(Select value)
            throws SqlObjectException {
	    if (select != value) {
            unassignItem(select);
            select = assignItem(value);
        }
	}

    public DMLFieldsAssignments getAssignments() {
	    return verifyNotNull(assignments);
	}

    public DMLFieldsAssignments newAssignments() {
	    if (assignments == null)
	        assignments = setOwner(new DMLFieldsAssignments());
		return assignments;
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        SqlQueryInsert targetInstance = (SqlQueryInsert) target;
        targetInstance.assignments = assignments == null ? null : targetInstance.setOwner((DMLFieldsAssignments) assignments.clone());
        targetInstance.select = select == null ? null : targetInstance.setOwner((Select) select.clone());
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
        else if (item.getClass() == Select.class) {
            if (this.select == null)
                this.select = (Select) item;
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
        else if (item == this.select) {
            unassignItem(this.select); this.select = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.assignments)
            this.assignments = (DMLFieldsAssignments) with;
        else if (what == this.select)
            this.select = (Select) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(assignments); assignments = null;
        unassignItem(select); select = null;
    }

    public SqlObject[] getSubItems () {
        return new SqlObject[]{params, assignments, select};
    }

}
