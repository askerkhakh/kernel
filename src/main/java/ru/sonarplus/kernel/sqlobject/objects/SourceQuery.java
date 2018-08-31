package ru.sonarplus.kernel.sqlobject.objects;
import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class SourceQuery extends Source {

    protected Select select = null;

    public SourceQuery() { super(); }

	public SourceQuery(Select select)
            throws SqlObjectException {
		this();
		setSelect(select);
	}

    public SourceQuery(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

	public SourceQuery(SqlObject owner, Select select)
            throws SqlObjectException {
		this(owner);
		setSelect(select);
	}

	@Override
	public String getTable() {
		return SqlObjectUtils.getRequestTableName(Preconditions.checkNotNull(select));
	}

	@Override
	public void setTable(String value) {}

	@Override
	public String getSchema() {
		return SqlObjectUtils.getRequestSchemaName(Preconditions.checkNotNull(select));
	}

	@Override
	public void setSchema(String value) {}
	
	public Select findSelect() { return select; }

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
        SourceQuery targetInstance = (SourceQuery) target;
        targetInstance.select = select == null ? null : targetInstance.setOwner((Select) select.clone());
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
