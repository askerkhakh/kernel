package ru.sonarplus.kernel.transaction;

interface DbTransaction {
    void start() throws Exception;
    void commit() throws Exception;
    void rollback() throws Exception;
}
