package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class CaseSearch extends Case {

    private WhenThenArray whenThen = null;

    protected class WhenThenArray extends SqlObjectsArrayInternal {

        public WhenThenArray(SqlObject owner) { super(owner); }

        @Override
        protected Class getItemClass () { return WhenThen.class; }

        @Override
        public SqlObject[] getSubItems () {
            if (items == null || items.size() == 0)
                return new SqlObject[1];
            else {
                SqlObject[] result = new SqlObject[items.size() + 1];
                for (int i = 0; i < items.size(); i++)
                    result[i] = items.get(i);
                return result;
            }
        }
    }

	public CaseSearch() { super();	}
    public CaseSearch(SqlObject owner)
            throws SqlObjectException { super(owner); }

    public static class WhenThen extends Case.WhenThen {

	    private Predicate caseWhen = null;

        public WhenThen (SqlObject owner)
                throws SqlObjectException {
            super(owner);
        }

        public WhenThen (SqlObject owner, Predicate when, ColumnExpression then)
                throws SqlObjectException {
            super(owner);
            setWhenThen(when, then);
        }

        public WhenThen (Predicate when, ColumnExpression then)
                throws SqlObjectException {
            super();
            setWhenThen(when, then);
        }

        protected void setWhenThen(Predicate when, ColumnExpression then)
                throws SqlObjectException {
            Preconditions.checkNotNull(when,
                    "Для объекта %s должно быть задано WHEN",
                    this.getClass().getSimpleName());

            Preconditions.checkNotNull(then,
                    "Для объекта %s должно быть задано THEN",
                    this.getClass().getSimpleName());
            setWhen(when);
            setThen(then);
        }

        public Predicate getWhen() {
            return caseWhen;
        }

        public void setWhen(Predicate value)
                throws SqlObjectException {
            if (this.caseWhen != value) {
                unassignItem(caseWhen);
                caseWhen = assignItem(value);
            }
        }

        @Override
        protected void internalClone(SqlObject target)
                throws CloneNotSupportedException {
            super.internalClone(target);
            ((WhenThen)target).caseWhen = caseWhen == null ? null : target.setOwner((Predicate)caseWhen.clone());
        }

        @Override
        protected void internalInsertItem(SqlObject item)
                throws SqlObjectException {
            if (item instanceof Predicate) {
                if (this.caseWhen == null)
                    this.caseWhen = (Predicate)item;
            }
            if (item instanceof ColumnExpression) {
                if (this.caseThen == null)
                    this.caseThen = (ColumnExpression)item;
            }
            else
                super.internalInsertItem(item);
        }

        @Override
        protected void internalRemoveItem (SqlObject item)
                throws SqlObjectException {
            if (item == this.caseWhen) {
                unassignItem(this.caseWhen); this.caseWhen = null;
            }
            else
                super.internalRemoveItem(item);
        }

        @Override
        protected void internalReplace (SqlObject what, SqlObject with)
                throws SqlObjectException {
            if (what == this.caseWhen)
                this.caseWhen = (Predicate) with;
            else
                super.internalReplace(what, with);
        }

        @Override
        protected void internalDestroyItems() {
            super.internalDestroyItems();
            unassignItem(this.caseWhen); this.caseWhen = null;
        }

        @Override
        public SqlObject[] getSubItems () { return new SqlObject[]{this.caseWhen, this.caseThen}; }

    }

	protected WhenThenArray newWhenThen() {
        if (whenThen == null)
            whenThen = new WhenThenArray(this);
        return whenThen;
    }

    public CaseSearch addWhenThen(Predicate when, ColumnExpression then)
            throws SqlObjectException {
        newWhenThen().insertItem(new WhenThen(when, then));
        return this;
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        CaseSearch targetInstance = (CaseSearch)target;
        targetInstance.whenThen = null;
        if (this.whenThen != null && this.whenThen.itemsCount() != 0)
            for (SqlObject item: this.whenThen) {
                SqlObject itemClone = item.clone();
                try {
                    targetInstance.newWhenThen().internalInsertItem(itemClone);
                    targetInstance.setOwner(itemClone);
                }
                catch (SqlObjectException e) {
                    Preconditions.checkState(false,
                            e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {
        if (item instanceof ColumnExpression) {
            if (this.caseElse == null)
                this.caseElse = (ColumnExpression) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == WhenThen.class)
            newWhenThen().internalInsertItem(item);
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item.getClass() == WhenThen.class)
            Preconditions.checkNotNull(whenThen).internalRemoveItem(item);
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        if (whenThen != null)
            whenThen.destroyItems();
    }

    @Override
    public SqlObject[] getSubItems () {
        if (whenThen == null)
            return new SqlObject[]{caseElse};

        SqlObject[] result = whenThen.getSubItems();
        result[result.length - 1] = caseElse;
	    return result;
	}

}
