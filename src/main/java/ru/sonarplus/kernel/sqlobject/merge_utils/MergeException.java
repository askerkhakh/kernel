package ru.sonarplus.kernel.sqlobject.merge_utils;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class MergeException extends SqlObjectException {

    public MergeException(String message) {
        super(message);
    }
}
