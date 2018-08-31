package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class Case extends ColumnExpression {
	protected ColumnExpression caseElse = null;

    public Case() {super(); }
    public Case(SqlObject owner)
            throws SqlObjectException { super(owner); }


    public ColumnExpression getElse() { return caseElse; }

    public void setElse(ColumnExpression value)
                throws SqlObjectException {
        if (this.caseElse != value) {
            unassignItem(caseElse);
            this.caseElse = assignItem(value);
        }
    }

    public static class WhenThen extends SqlObject {
	    protected ColumnExpression caseThen = null;

        public WhenThen () { super(); }
        public WhenThen (SqlObject owner)
                throws SqlObjectException { super(owner); }

        public ColumnExpression getThen() {
        	return caseThen;
        }

        public void setThen(ColumnExpression value)
                throws SqlObjectException {
            if (this.caseThen != value) {
                unassignItem(caseThen);
                this.caseThen = assignItem(value);
            }
        }

        @Override
        protected void internalClone(SqlObject target)
                throws CloneNotSupportedException {
            super.internalClone(target);
            ((WhenThen)target).caseThen = this.caseThen == null ? null : target.setOwner((ColumnExpression)this.caseThen.clone());
        }

        @Override
        protected void internalRemoveItem (SqlObject item)
                throws SqlObjectException {
            if (item == this.caseThen) {
                unassignItem(this.caseThen); this.caseThen = null;
            }
            else
                super.internalRemoveItem(item);
        }

        @Override
        protected void internalReplace (SqlObject what, SqlObject with)
                throws SqlObjectException {
            if (what == this.caseThen)
                this.caseThen = (ColumnExpression) with;
            else
                super.internalReplace(what, with);
        }

        @Override
        protected void internalDestroyItems() {
            super.internalDestroyItems();
    	    unassignItem(caseThen); caseThen = null;
        }

    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        ((Case)target).caseElse = this.caseElse == null? null : target.setOwner((ColumnExpression)caseElse.clone());
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.caseElse) {
            unassignItem(this.caseElse); this.caseElse = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.caseElse)
            this.caseElse = (ColumnExpression) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(caseElse); caseElse = null;
    }

}
