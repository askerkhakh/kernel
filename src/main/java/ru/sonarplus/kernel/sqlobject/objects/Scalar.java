package ru.sonarplus.kernel.sqlobject.objects;

/* TScalar - скалярный, т.е. возвращающий одну запись с одной колонкой, запрос.
    Содержащийся в данном выражении запрос - обычный TSelect.
    разница только в том, что при дистиляции принудительно будет устанавливаться ограничение на количество возвращаемых записей = 1.
    Также при дистиляции будет проверяться количество возвращаемых колонок */

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class Scalar extends ColumnExpression {

    protected Select select = null;

    public Scalar() { super(); }

    public Scalar(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

    public Select findSelect() { return select; }

    public Select getSelect() { return Preconditions.checkNotNull(select); }

    public void setSelect(Select value)
            throws SqlObjectException {
        if (select != value) {
            unassignItem(select);
            select = assignItem(value);
        }
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        Scalar targetInstance = (Scalar) target;
        targetInstance.select = select == null ? null : target.setOwner((Select) select.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == Select.class) {
            if (this.select == null)
                this.select = (Select) item;
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
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.select)
            this.select = (Select) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(select); select = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{select};
    }

}
