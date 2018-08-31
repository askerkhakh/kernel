package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateComparisonCommon extends Predicate {

    protected ColumnExpression left = null;
    protected ColumnExpression right = null;

    public PredicateComparisonCommon() { super(); }
    public PredicateComparisonCommon(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

	public ColumnExpression getLeft() { return left; }

    public void setLeft(ColumnExpression value)
            throws SqlObjectException {
        if (left != value) {
            unassignItem(left);
            left = assignItem(value);
        }
    }

    public ColumnExpression getRight() {
		return right;
	}

    public void setRight(ColumnExpression value)
            throws SqlObjectException {
        if (right != value) {
            unassignItem(right);
            right = assignItem(value);
        }
    }

    protected void setFirstEmptyField(ColumnExpression item)
            throws SqlObjectException {
        if (left == null)
            left = item;
        else if (right == null)
            right = item;
        else
            throw new SqlObjectException(String.format(
                    "Не удалось добавить в '%s' подчинённый объект '%' - все поля уже установлены",
                    getClass().getSimpleName(),
                    item.getClass().getSimpleName()
            ));
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        PredicateComparisonCommon targetInstance = (PredicateComparisonCommon) target;
        targetInstance.left = left == null ? null : target.setOwner((ColumnExpression) left.clone());
        targetInstance.right = right == null ? null : target.setOwner((ColumnExpression) right.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item instanceof ColumnExpression) {
            if (this.left == null)
                this.left = (ColumnExpression)item;
            else if (this.right == null)
                this.right = (ColumnExpression)item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.left) {
            unassignItem(this.left); this.left = null;
        }
        else if (item == this.right) {
            unassignItem(this.right); this.right = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.left)
            this.left = (ColumnExpression) with;
        else if (what == this.right)
            this.right = (ColumnExpression) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(left); left = null;
        unassignItem(right); right = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{left, right};
    }

}
