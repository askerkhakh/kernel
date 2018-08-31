package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateExists extends Predicate {

    protected Select select = null;

    public PredicateExists() { super(); }

    public PredicateExists(Select select)
            throws SqlObjectException {
        this();
        setSelect(select);
    }

    public PredicateExists(SqlObject owner)
            throws SqlObjectException { super(owner); }

    public PredicateExists(SqlObject owner, Select select)
            throws SqlObjectException {
        this(owner);
        setSelect(select);
    }

	public Select findSelect() {
		return select;
	}
	
	public Select getSelect() { return Preconditions.checkNotNull(select); }

	public void setSelect(Select value)
            throws SqlObjectException {
		if (select != value) {
		    unassignItem(select);
		    select = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        PredicateExists targetInstance = (PredicateExists) target;
        targetInstance.select = select == null ? null : target.setOwner((Select) select.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == Select.class) {
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
        if (item == this.select) {
            unassignItem(this.select); this.select = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.select)
            this.select = (Select) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(select); select = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{select};
    }

}
