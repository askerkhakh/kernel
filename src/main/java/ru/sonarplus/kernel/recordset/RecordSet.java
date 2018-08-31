package ru.sonarplus.kernel.recordset;


/**
 * Набор записей. По сути обёртка над {@link java.sql.ResultSet}. Основная задача - конвертировать СУБД-зависимые данные
 * из ResultSet в нативные Java значения, отработав по пути нашу специфику - то как мы храним данные разных типов в
 * разных СУБД и поддержку "наших null'ов".
 */
public interface RecordSet extends Record, AutoCloseable{
    boolean isEmpty();
    // TODO добавить методы чтения следующей записи и т.п.
}
