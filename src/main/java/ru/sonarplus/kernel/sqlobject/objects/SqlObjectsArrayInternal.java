package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;

public abstract class SqlObjectsArrayInternal extends SqlObjectsArray {

    public SqlObjectsArrayInternal(SqlObject owner) { super(); this.owner = owner; }

    @Override
    protected SqlObject getOwnerForItems() { return Preconditions.checkNotNull(owner); }
}
