package ru.sonarplus.kernel.transaction;

/**
 * Реализация интерфейса транзакции, которая ничего не делает
 */
class DummyDbTransaction implements DbTransaction {
    @Override
    public void start() {}

    @Override
    public void commit() {}

    @Override
    public void rollback() {}
}