package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class SelectedColumnsContainer extends SqlObjectsArray {

    public SelectedColumnsContainer() { super(); }

    public SelectedColumnsContainer(SqlObject owner) throws SqlObjectException {
        super(owner);
    }

    @Override
    protected Class getItemClass () { return SelectedColumn.class; }

	public SelectedColumnsContainer addColumn(SelectedColumn column)
            throws SqlObjectException {
		if (column != null)
			insertItem(column);
		return this;
	}
	
	public SelectedColumnsContainer addColumn(ColumnExpression expr, String alias)
            throws SqlObjectException {
		if (expr != null)
			new SelectedColumn(this, expr, alias);
		return this;
	}
	
	public SelectedColumn findColumnByAlias(String alias) {
		for (SqlObject item: this) {
			SelectedColumn column = (SelectedColumn) item;
			if ((column.alias != null) && column.alias.equals(alias))
				return column;
		}
		return null;
	}
	
	public SelectedColumn getColumn(int index){
		return (SelectedColumn) getItem(index);
	}
	
}
