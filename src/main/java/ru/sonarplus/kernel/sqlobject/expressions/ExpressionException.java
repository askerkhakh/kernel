package ru.sonarplus.kernel.sqlobject.expressions;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class ExpressionException extends SqlObjectException {

        public ExpressionException(String message) {
            super(message);
        }
}
