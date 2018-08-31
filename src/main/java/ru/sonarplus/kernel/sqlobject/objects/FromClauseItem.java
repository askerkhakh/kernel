package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;


public class FromClauseItem extends SqlObject {

    protected TableExpression tableExpr = null;
    protected Join join = null;

	public FromClauseItem() { super(); }

    public FromClauseItem(SqlObject owner)
			throws SqlObjectException {
        super(owner);
    }

	public FromClauseItem(SqlObject owner,
			TableExpression expression,
			Join join)
            throws SqlObjectException {
		this(owner);
		setTableExpr(expression);
		setJoin(join);
	}
	
	public TableExpression getTableExpr() {
	    if (tableExpr == null)
	        tableExpr = setOwner(new TableExpression());
	    return tableExpr;
	}

	public void setTableExpr(TableExpression value)
            throws SqlObjectException {
        if (tableExpr != value) {
            unassignItem(tableExpr);
            tableExpr = assignItem(value);
        }
	}

	public String getAlias() {
		return getTableExpr().alias;
	}
	
	public void setAlias(String value) {
		getTableExpr().alias = value;
	}
	
	public String getTableName() {
		return Preconditions.checkNotNull(getTableExpr().getSource()).getTable();
	}
	
	public String getAliasOrName() {
		String result = getAlias();
		if (StringUtils.isEmpty(result)) {
			result = getTableName();
		}
		return result;
	}

	public boolean getIsFirst() {
	    return owner != null && ((FromContainer)owner).firstSubItem() == this;
	}
	
	public boolean getIsJoined() {
		return ((owner != null) && !getIsFirst());
		//TODO || getJoin() != null);
	}
	
	public Join getJoin() {
		return join;
	}

	public void setJoin(Join value)
            throws SqlObjectException {
        if (this.join != value) {
            unassignItem(this.join);
            this.join = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        FromClauseItem targetInstance = (FromClauseItem) target;
        targetInstance.tableExpr = this.tableExpr == null ? null : targetInstance.setOwner((TableExpression) this.tableExpr.clone());
        targetInstance.join = this.join == null ? null : targetInstance.setOwner((Join) this.join.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == TableExpression.class) {
            if (this.tableExpr == null)
                this.tableExpr = (TableExpression) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == Join.class) {
            if (this.join == null)
                this.join = (Join) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.tableExpr) {
            unassignItem(this.tableExpr); this.tableExpr = null;
        }
        else if (item == this.join) {
            unassignItem(this.join); this.join = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.tableExpr)
            this.tableExpr = (TableExpression) with;
        else if (what == this.join)
            this.join = (Join) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(tableExpr); tableExpr = null;
        unassignItem(join); join = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{tableExpr, join};
    }

}
