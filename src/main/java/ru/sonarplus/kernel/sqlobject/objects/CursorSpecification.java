package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import javax.annotation.Nullable;

/* TCursorSpecification:
    SELECT + ORDER BY + fetch statement согласно БНФ
  */

public class CursorSpecification extends SqlQuery {

	private Select select = null;
	private OrderBy orderBy = null;
	private long fetchOffset;
	private long fetchFirst;

    public CursorSpecification() { super(); }

	public CursorSpecification(@Nullable SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}

    public OrderBy findOrderBy() { return orderBy; }

    public OrderBy newOrderBy() {
        if (orderBy == null)
            orderBy = setOwner(new OrderBy());
        return orderBy;
    }

    public OrderBy getOrderBy() { return Preconditions.checkNotNull(this.orderBy); }

    public CursorSpecification setOrderBy(OrderBy value)
            throws SqlObjectException {
        if (orderBy != value) {
            unassignItem(orderBy);
            orderBy = assignItem(value);
        }
        return this;
    }

    public Select findSelect() { return select; }

    public Select newSelect() {
        if (select == null)
            select = setOwner(new Select());
        return select;
    }

    public Select getSelect() { return Preconditions.checkNotNull(select); }

    public CursorSpecification setSelect(Select value) {
        if (select != value) {
            unassignItem(select);
            select = setOwner(value);
        }
        return this;
    }

	public boolean isOrdered() {
        return orderBy != null && orderBy.isHasChilds();
	}

	protected void moveParamsToSelect(Select select, boolean clone)
            throws SqlObjectException, CloneNotSupportedException {
		QueryParams params = getParams();
		if (params == null)
			return;

		QueryParams newParams = select.newParams();
		if (clone)
            for (SqlObject item: params)
                newParams.insertItem(item.clone());
        else
            for (SqlObject item: params)
                newParams.insertItem(item);
	}
	
	public Select toSelect(boolean clone)
            throws CloneNotSupportedException, SqlObjectException {
		Select result = getSelect();
		if (clone)
			result = (Select) result.clone();
		moveParamsToSelect(result, clone);
		if (!clone)
			removeItem(result);
		return result;
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        CursorSpecification targetCursor = (CursorSpecification) target;
        targetCursor.select = select == null ? null : target.setOwner((Select) select.clone());
        targetCursor.orderBy = orderBy == null ? null : target.setOwner((OrderBy) orderBy.clone());
    }

    @Override
    protected void internalInsertItem(SqlObject item)
            throws SqlObjectException {
        if (item.getClass() == Select.class) {
            if (this.select == null)
                this.select = (Select) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == OrderBy.class) {
            if (this.orderBy == null)
                this.orderBy = (OrderBy) item;
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
        else if (item == this.orderBy) {
            unassignItem(this.orderBy); this.orderBy = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.select)
            this.select = (Select) with;
        else if (what == this.orderBy)
            this.orderBy = (OrderBy) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(select); select = null;
        unassignItem(orderBy); orderBy = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{params, select, orderBy};
    }

    public long getFetchOffset() {
        return fetchOffset;
    }

    public CursorSpecification setFetchOffset(long fetchOffset) {
        this.fetchOffset = fetchOffset;
        return this;
    }

    public long getFetchFirst() {
        return fetchFirst;
    }

    public CursorSpecification setFetchFirst(long fetchFirst) {
        this.fetchFirst = fetchFirst;
        return this;
    }
}
