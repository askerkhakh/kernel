package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class CaseSimple extends Case {
	protected ColumnExpression caseCase = null;

    private WhenThenArray whenThen = null;

    protected class WhenThenArray extends SqlObjectsArrayInternal {

        public WhenThenArray(SqlObject owner) { super(owner); }

        @Override
        protected Class getItemClass () { return WhenThen.class; }

        @Override
        public SqlObject[] getSubItems () {
            if (items == null || items.size() == 0)
                return new SqlObject[2];
            else {
                SqlObject[] result = new SqlObject[items.size() + 2];
                for (int i = 0; i < items.size(); i++)
                    result[1 + i] = items.get(i);
                return result;
            }
        }
    }

    public CaseSimple() { super(); }
    public CaseSimple(SqlObject owner)
            throws SqlObjectException { super(owner); }


    public static class WhenThen extends Case.WhenThen {
        private ColumnExpression caseWhen = null;

        public WhenThen (SqlObject owner, ColumnExpression when, ColumnExpression then)
                throws SqlObjectException {
            super(owner);
            Preconditions.checkNotNull(when,
                    "Для объекта %s должно быть задано WHEN",
                    this.getClass().getSimpleName());

            Preconditions.checkNotNull(then,
                    "Для объекта %s должно быть задано THEN",
                    this.getClass().getSimpleName());
            setWhen(when);
            setThen(then);
        }

        public WhenThen (ColumnExpression when, ColumnExpression then)
                throws SqlObjectException {
            super();
            Preconditions.checkNotNull(when,
                    "Для объекта %s должно быть задано WHEN",
                    this.getClass().getSimpleName());

            Preconditions.checkNotNull(then,
                    "Для объекта %s должно быть задано THEN",
                    this.getClass().getSimpleName());
            setWhen(when);
            setThen(then);
        }

    	public ColumnExpression getWhen() {
    		return caseWhen;
    	}

    	public void setWhen(ColumnExpression value)
                throws SqlObjectException {
            if (this.caseWhen != value) {
                unassignItem(caseWhen);
                this.caseWhen = assignItem(value);
            }
    	}

        @Override
        protected void internalClone(SqlObject target)
                throws CloneNotSupportedException {
            super.internalClone(target);
            ((WhenThen)target).caseWhen = caseWhen == null ? null : target.setOwner((ColumnExpression) caseWhen.clone());
        }

        @Override
        protected void internalInsertItem(SqlObject item)
                throws SqlObjectException {
            if (item instanceof ColumnExpression) {
                if (this.caseWhen == null)
                    this.caseWhen = (ColumnExpression)item;
                else if (this.caseThen == null)
                    this.caseThen = (ColumnExpression)item;
                else
                    super.internalInsertItem(item);
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
                this.caseWhen = (ColumnExpression) with;
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

	public ColumnExpression getCase() {
		return caseCase;
	}

	public void setCase(ColumnExpression value)
            throws SqlObjectException {
        if (this.caseCase != value) {
            unassignItem(caseCase);
            caseCase = assignItem(value);
        }
	}

    protected WhenThenArray newWhenThen() {
        if (whenThen == null)
            whenThen = new WhenThenArray(this);
        return whenThen;
    }

    public CaseSimple addWhenThen(ColumnExpression when, ColumnExpression then)
            throws SqlObjectException {
        newWhenThen().insertItem(new WhenThen(when, then));
        return this;
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        CaseSimple targetInstance = (CaseSimple)target;
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
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.caseCase)
            this.caseCase = (ColumnExpression) with;
        else
            super.internalReplace(what, with);
    }

    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {
        if (item instanceof ColumnExpression) {
            if (this.caseCase == null)
                this.caseCase = (ColumnExpression) item;
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
        if (item == this.caseCase) {
            unassignItem(this.caseCase); this.caseCase = null;
        }
        else if (item.getClass() == WhenThen.class)
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
            return new SqlObject[]{caseCase, caseElse};

        SqlObject[] result = whenThen.getSubItems();
        result[0] = caseCase;
        result[result.length - 1] = caseElse;
        return result;
    }

}
