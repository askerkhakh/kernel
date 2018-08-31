package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class UnionsContainer extends SqlObjectsArray {

    public UnionsContainer() { super(); }

    public UnionsContainer(SqlObject owner) throws SqlObjectException {
        super(owner);
    }

    @Override
    protected Class getItemClass() { return UnionItem.class; }

	public void add(UnionItem item)
            throws SqlObjectException {
		insertItem(item);
	}
	
	public void add(Select select, UnionItem.UnionType unionType)
            throws SqlObjectException {
		new UnionItem(this, select, unionType);
	}
	
	public void add(Select select)
            throws SqlObjectException {
		add(select, UnionItem.UnionType.UNION_ALL);
	}
	
}
