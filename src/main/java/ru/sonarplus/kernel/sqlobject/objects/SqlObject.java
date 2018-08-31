package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import javax.annotation.Nullable;
import java.util.Iterator;

public class SqlObject implements Cloneable, Iterable<SqlObject> {

    @Nullable
    protected SqlObject owner = null;

    public SqlObject () {}

    SqlObject (SqlObject owner)
            throws SqlObjectException {
        this();
        if (owner != null)
            owner.insertItem(this);
    }

    public final void insertItem(SqlObject item)
            throws SqlObjectException {
        if ((item == null) || (item.owner == getOwnerForItems()) || (item == getOwnerForItems()))
            return;
        checkCycle(item);
        if (item.owner != null)
            item.owner.removeItem(item);
        internalInsertItem(item);
        setOwner(item);
    }

    public final void removeFromOwner()
            throws SqlObjectException{
        if (this.owner != null)
            this.owner.removeItem(this);
    }

    public final void removeItem(SqlObject item)
            throws SqlObjectException {
        if ((item == null) || (item.owner != getOwnerForItems()) ||(item == getOwnerForItems()))
            return;
        internalRemoveItem(item);
        unassignItem(item);
    }

    public final void destroyItems() { internalDestroyItems(); }

    public final void replace(SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == null)
            throw new SqlObjectException(
                    String.format(
                            "Замена подчинённой ссылки для '%s': не указана (null) заменяемая ссылка",
                            this.getClass().getSimpleName()
                    )
            );
        if (what.owner != getOwnerForItems())
            throw new SqlObjectException(
                    String.format(
                            "В массиве '%s' не удалось выполнить замену подчинённого - заменяемый объект не является подчинённым",
                            this.getClass().getSimpleName()
                    )
            );
        if (what == with)
            return;
        checkCycle(with);
        if (with != null) {
            if (with.owner != null)
                with.owner.removeItem(with);
        }
        internalReplace(what, with);
        unassignItem(what);
        setOwner(with);
    }

    protected SqlObject getOwnerForItems() {return this; }

    protected final <T extends SqlObject> T setOwner (T item) {
        item.owner = getOwnerForItems();
        return item;
    }

    protected final <T extends SqlObject> T assignItem (T item)
            throws SqlObjectException {
        if (item != null) {
            if (item.owner != getOwnerForItems())
                checkCycle(item);
            if (item.owner != null)
                item.owner.removeItem(item);
            return setOwner(item);
        }
        return item;
    }

    protected final void unassignItem (@Nullable SqlObject item) {
        if (item != null)
            item.owner = null;
    }

    public SqlObject getOwner(){ return owner; }

    public class SqlObjectIterator implements Iterator {
        private SqlObject[] items = null;
        private int index = 0;

        public SqlObjectIterator(SqlObject iterable) {
            items = Preconditions.checkNotNull(iterable.getSubItems());
            index = -1;
            moveToNextNonNull();
        }

        protected void moveToNextNonNull() {
            this.index++;
            while (this.index < items.length && items[this.index] == null)
                this.index++;
        }
        @Override
        public boolean hasNext() {return index < items.length;}

        @Override
        public SqlObject next() {
            int current = this.index;
            moveToNextNonNull();
            return items[current];
        }
    }

    //<Iterable>
    public Iterator<SqlObject> iterator() {
        return new SqlObjectIterator(this);
    }
    //</Iterable>

    protected final void checkCycle(SqlObject child)
            throws SqlObjectException {
        if (child == null)
            return;
        SqlObject item = this.owner;
        while ((item != null) && (item != child))
            item = item.owner;
        if (item != null) // item==child
            throw new SqlObjectException(
                    String.format(
                            "Попытка добавить в подчинение объекту 1(%s) объект 2 (%s), являющийся вершиной дерева, содержащего объект 1",
                            this.getClass().getSimpleName(),
                            child.getClass().getSimpleName()
                    )
            ); // цикл обнаружили
    }

    @Override
    public SqlObject clone() {
        try {
            SqlObject result = (SqlObject) super.clone();
            result.owner = null;
            internalClone(result);
            return result;
        }
        catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {}

    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {
        throw new SqlObjectException(
                String.format(
                        "Не удалось непосредственное добавить объект '%s', в подчинение объекту '%s'",
                        item.getClass().getName(),
                        getOwnerForItems().getClass().getName())
        );
    }

    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        throw new SqlObjectException(
                String.format(
                        "Не поддержано непосредственное удаление объекта '%s', из подчинения объекту '%s'",
                        item.getClass().getName(),
                        getOwnerForItems().getClass().getName())
        );
    }

    protected void internalReplace (SqlObject what, SqlObject with) throws SqlObjectException {
        throw new SqlObjectException(
                String.format(
                        "Для объекта '%s' не удалось выполнить замену подчинённого объекта ссылкой '%s'",
                        getOwnerForItems().getClass().getSimpleName(),
                        what.getClass().getSimpleName()
                )
        );
    }

    protected void internalDestroyItems() {}

    protected SqlObject[] getSubItems () { return new SqlObject[]{}; }
}
