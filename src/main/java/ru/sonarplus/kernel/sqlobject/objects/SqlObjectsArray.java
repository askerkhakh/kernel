package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.ArrayList;
import java.util.List;

public abstract class SqlObjectsArray extends SqlObject {

    protected List<SqlObject> items = null;

    public SqlObjectsArray () {super();}
    public SqlObjectsArray (SqlObject owner)
            throws SqlObjectException {super(owner);}

    protected List<SqlObject> newItems () {
        if (items == null)
            items = new ArrayList<>();
        return items;
    }

    protected abstract Class getItemClass ();

    public final void replaceWithSet(SqlObject what, SqlObject...with)
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
        int index = indexOf(what);
        if (index >= 0) {
            removeItem(what);
            for (SqlObject item: with)
                if (item != null)
                    insertItem(item, index++);
        }
        else
            throw new SqlObjectException(
                    String.format(
                            "В массиве '%s' не удалось выполнить замену подчинённого - заменяемый объект не содержится в списке",
                            this.getClass().getSimpleName()
                    )
            );
    }

    @Override
    protected final void internalInsertItem (SqlObject item)
            throws SqlObjectException {
        if (getItemClass().isAssignableFrom(item.getClass()))
            newItems().add(item);
        else
            super.internalInsertItem(item);
    }

    protected static final boolean replaceReferenceInArray (SqlObject what, SqlObject with, List items) {
        if (items == null)
            return false;

        int index = items.indexOf(what);
        if (index < 0)
            return false;

        items.set(index, with);
        return true;
    }


    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (getItemClass().isAssignableFrom(with.getClass())) {
            if (!replaceReferenceInArray(what, with, items))
                super.internalReplace(what, with);
        }
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalRemoveItem(SqlObject item)
            throws SqlObjectException {
        Preconditions.checkNotNull(items).remove(item);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        if (items != null) {
            items.forEach((item)->{item.owner = null;});
            items.clear();
        }
    }

    public final boolean isHasChilds() { return itemsCount() > 0; }

    public final int indexOf(SqlObject item) {
        if (item == null || items == null)
            return -1;
        return items.indexOf(item);
    }

    public final int itemsCount() {
        if (items == null)
            return 0;
        else
            return items.size();
    }

    public final SqlObject getItem(int index) {
        return Preconditions.checkNotNull(items).get(index);
    }

    public SqlObject firstSubItem() {
        if (items == null || items.size() == 0)
            return null;
        else
            return items.get(0);
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        SqlObjectsArray targetInstance = (SqlObjectsArray)target;
        targetInstance.items = null;
        if (this.items != null)
            for(SqlObject sourceItem: this)
                targetInstance.newItems().add(targetInstance.setOwner(sourceItem.clone()));
    }

    public final void insertItem(SqlObject item, int index)
            throws SqlObjectException {
        if ((item == null) || (item.owner == getOwnerForItems()) || (item == getOwnerForItems()))
            return;
        if (indexOf(item) >= 0)
            return;

        if (!getItemClass().isAssignableFrom(item.getClass()))
            throw new SqlObjectException(String.format(
                    "Объект '%s' не может содержать подчинённые '%s'",
                    getClass().getSimpleName(),
                    item.getClass().getSimpleName()
            ));

        checkCycle(item);
        if (item.owner != null)
            item.owner.removeItem(item);

        newItems().add(index, item);

        setOwner(item);
    }

    @Override
    public SqlObject[] getSubItems () {
        if (items != null && items.size() != 0)
            return items.toArray(new SqlObject[0]);
        else
            return new SqlObject[0];
    }

}
