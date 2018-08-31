package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class DMLFieldsAssignments extends SqlObjectsArray {

    public DMLFieldsAssignments() { super(); }

    public DMLFieldsAssignments(SqlObject owner) throws SqlObjectException {
        super(owner);
    }

    @Override
    protected Class getItemClass () { return DMLFieldAssignment.class; }

}
