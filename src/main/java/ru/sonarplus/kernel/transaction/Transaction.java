package ru.sonarplus.kernel.transaction;

/**
 * Интерфейс транзакции. Транзакция стартуется при получении, поэтому её можно только принять либо откатить.
 */
public interface Transaction {
    void commit() throws Exception;
    void rollback() throws Exception;

    // FIXME #45200 удалить
    int getId();
}