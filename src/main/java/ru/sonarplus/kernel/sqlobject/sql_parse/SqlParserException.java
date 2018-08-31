package ru.sonarplus.kernel.sqlobject.sql_parse;

public class SqlParserException extends Exception {
    public static final String INVALID_UNARY_OPERATION = "Неверная унарная операция '%s'";
    public static final String INVALID_SEMICOLON = "Символ ':' перед квалифицированным идентификатором '%s.%s'";
    public SqlParserException(String message) {
        super(message);
    }
}
