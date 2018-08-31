package ru.sonarplus.kernel.sqlobject.distillation;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class DistillationException extends SqlObjectException {
    public DistillationException(String message) {
        super(message);
    }
}