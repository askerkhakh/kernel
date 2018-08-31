package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class TableExpression extends SqlObject {

    protected Source source = null;

	public String alias;

    public TableExpression() { super(); }
    public TableExpression(Source source, String alias)
            throws SqlObjectException {
        this();
        setSource(source);
        this.alias = alias;
    }

	public TableExpression(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}
	
	public TableExpression(SqlObject owner, Source source, String alias)
            throws SqlObjectException {
		this(owner);
		setSource(source);
		this.alias = alias;
	}
	
	public Source getSource() {
		return source;
	}

	public void setSource(Source value)
            throws SqlObjectException {
        if (source != value) {
            unassignItem(source);
            source = assignItem(value);
        }
	}

	public String getTableName() {
		if (source != null)
            return source.getTable();
		else
            return "";
	}

	public void setTableName(String value) {
		if (source != null)
		    unassignItem(source); source = null;
		source = setOwner(new SourceTable(value));
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        TableExpression targetInstance = (TableExpression) target;
        targetInstance.source = source == null ? null : targetInstance.setOwner((Source) source.clone());
    }


    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == SourceTable.class || item.getClass() == SourceQuery.class) {
            if (this.source == null)
                this.source = (Source) item;
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.source) {
            unassignItem(this.source); this.source = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.source)
            this.source = (Source) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(source); source = null;
    }

    public SqlObject[] getSubItems () {
        return new SqlObject[]{source};
    }

}
