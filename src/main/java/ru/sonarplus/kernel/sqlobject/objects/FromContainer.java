package ru.sonarplus.kernel.sqlobject.objects;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class FromContainer extends SqlObjectsArray {

    public FromContainer() { super(); }

    public FromContainer(SqlObject owner) throws SqlObjectException {
        super(owner);
    }

    @Override
    protected Class getItemClass () { return FromClauseItem.class; }

	public FromContainer addQuery(Select select, String alias,
			Join join)
            throws SqlObjectException {
		FromClauseItem item = new FromClauseItem(null, 
				new TableExpression(null, 
						new SourceQuery(null, select), alias), join);
		insertItem(item);
		return this;
	}
	
	public FromContainer addTable(String table, String alias,
			Join join)
            throws SqlObjectException {
		FromClauseItem item = new FromClauseItem(null,
				new TableExpression(null, 
						new SourceTable(null, table), alias), join);
		
		insertItem(item);
		return this;
	}

	public FromContainer addTable(String table, String alias)
            throws SqlObjectException {
		return addTable(table, alias, null);
	}
	
	protected boolean isSameAliasOrName(String aliasOrName, FromClauseItem item) {
		if (!StringUtils.isEmpty(item.getTableExpr().alias)) {
			return aliasOrName.equals(item.getTableExpr().alias);
		}
		else if (item.getTableExpr().getSource() instanceof SourceTable) {
			SourceTable source = (SourceTable) item.getTableExpr().getSource();
			return aliasOrName.equals(source.table);
		}
		else {
			return false;
		}
	}
	
	public FromClauseItem findItem(String aliasOrName){
		if (!isHasChilds()) {
			return null;
		}
		if (StringUtils.isEmpty(aliasOrName))
            return (FromClauseItem) firstSubItem();
		else {
			for (SqlObject item: this) {
				FromClauseItem fromItem = (FromClauseItem) item;
				if (isSameAliasOrName(aliasOrName,  fromItem))
					return fromItem;
			}
		}
		return null;
	}
	
}
