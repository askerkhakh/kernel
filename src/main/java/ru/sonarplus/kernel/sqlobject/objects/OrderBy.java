package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

// Перечень элементов сортировки запроса
public class OrderBy extends SqlObjectsArray {

    public OrderBy() { super(); }

    public OrderBy(SqlObject owner) throws SqlObjectException {
        super(owner);
    }

    @Override
    protected Class getItemClass() { return OrderByItem.class; }

	public OrderBy addOrder(OrderByItem item, boolean isNeedFirst)
            throws SqlObjectException {
        if (isNeedFirst)
            insertItem(item, 0);
        else
            insertItem(item);
		return this;
	}
	
	public OrderBy addOrder(OrderByItem item)
            throws SqlObjectException {
		return addOrder(item, false);
	}

    public OrderBy addOrder(ColumnExpression expr, OrderByItem.OrderDirection direction, OrderByItem.NullOrdering nullOrdering)
            throws SqlObjectException {
        OrderByItem orderItem = new OrderByItem(this);
        orderItem.setExpr(expr);
        orderItem.direction = direction;
        orderItem.nullOrdering = nullOrdering;
        return this;
    }

    public OrderBy addOrder(ColumnExpression expr, OrderByItem.OrderDirection direction)
            throws SqlObjectException {
        return this.addOrder(expr, direction, OrderByItem.NullOrdering.NONE);
    }

    public OrderBy addOrder(ColumnExpression expr)
            throws SqlObjectException {
        return this.addOrder(expr, OrderByItem.OrderDirection.ASC, OrderByItem.NullOrdering.NONE);
    }

}
