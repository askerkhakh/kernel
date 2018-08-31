package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class Conditions extends Predicate {

    public enum BooleanOp {
        AND, OR
    }

    private PredicatesArray items = null;

    protected class PredicatesArray extends SqlObjectsArrayInternal {

        public PredicatesArray(SqlObject owner) { super(owner); }

        @Override
        protected Class getItemClass () { return Predicate.class; }

    }

	public BooleanOp booleanOp = BooleanOp.AND;

	public Conditions() { super(); }
    public Conditions(BooleanOp booleanOp) {
        this();
        this.booleanOp = booleanOp;
    }

    public Conditions(SqlObject owner)
            throws SqlObjectException { super(owner); }

    public Conditions(SqlObject owner, BooleanOp booleanOp)
            throws SqlObjectException {
        this(owner);
        this.booleanOp = booleanOp;
    }

    protected PredicatesArray newItems () {
	    if (items == null)
            items = new PredicatesArray(this);
	    return items;
    }

	public Conditions addCondition(Predicate condition)
            throws SqlObjectException {
	    if (condition == null)
	        return this;

	    newItems().insertItem(condition);

		return this;
	}

	protected Conditions extractAllChilds()
            throws SqlObjectException {
		Conditions result = new Conditions(booleanOp);
        for (SqlObject predicate: this)
            result.insertItem(predicate);
		return result;
	}

	public Conditions addCondition(Predicate condition, 
			BooleanOp newBooleanOp)
            throws SqlObjectException {
		if (newBooleanOp == booleanOp) {
			addCondition(condition);
		}
		else if (itemsCount() <= 1) {
			booleanOp = newBooleanOp;
			addCondition(condition);
		}
		else {
			Conditions sub = extractAllChilds();
			booleanOp = newBooleanOp;
			addCondition(sub);
			addCondition(condition);
		}
		return this;
	}
	
	public Conditions addConditions(BooleanOp booleanOp, Predicate... conditions)
            throws SqlObjectException {
		Conditions condOwner;
		if (this.booleanOp == booleanOp) {
			condOwner = this;
		}
		else {
			condOwner = new Conditions(booleanOp);
			this.addCondition(condOwner);
		}
		for (Predicate predicate: conditions){
			condOwner.addCondition(predicate);
		}
		return this;
	}

    public final boolean isHasChilds() { return itemsCount() > 0; }

	public boolean isEmpty() {
		if (!isHasChilds())
			return true;
		for (SqlObject item: this) {
			if (item instanceof Conditions)
			    return ((Conditions) item).isEmpty();
			else
				return false;
		}
		return true;
	}

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        Preconditions.checkNotNull(items).internalReplace(what, with);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
	    Preconditions.checkNotNull(items).internalRemoveItem(item);
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {
	    newItems().internalInsertItem(item);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        if (items != null)
            items.destroyItems();
    }

    public int indexOf(SqlObject item) {
        if (items == null)
            return -1;
        else
            return items.indexOf(item);
    }

    public int itemsCount() {
        if (items == null)
            return 0;
        else
            return items.itemsCount();
    }

    public final SqlObject getItem(int index)
            throws SqlObjectException {
        if (items == null)
            throw new SqlObjectException("");
        return items.getItem(index);
    }

    public SqlObject firstSubItem() {
        if (items == null)
            return null;
        else
            return items.firstSubItem();
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        Conditions targetInstance = (Conditions)target;
        targetInstance.items = null;
        if (this.items != null && this.itemsCount() != 0)
            for (SqlObject item: this.items) {
                SqlObject itemClone = item.clone();
                try {
                    targetInstance.newItems().internalInsertItem(itemClone);
                    targetInstance.setOwner(itemClone);
                }
                catch (SqlObjectException e) {
                    Preconditions.checkState(false,
                            e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
    }

    public final void insertItem(SqlObject item, int index)
            throws SqlObjectException {
        if ((item == null) || (item.owner == this) || (item == this))
            return;
        if (indexOf(item) >= 0)
            return;

        if (!(item instanceof Predicate))
            throw new SqlObjectException(String.format(
                    "Объект '%s' не может содержать подчинённые '%s'",
                    getClass().getSimpleName(),
                    item.getClass().getSimpleName()
            ));

        checkCycle(item);
        if (item.owner != null)
            item.owner.removeItem(item);

        newItems().insertItem(item, index);
    }

    @Override
    public SqlObject[] getSubItems () {
	    if (items != null)
	        return items.getSubItems();
	    else
	        return new SqlObject[]{};
	}

}
