package ru.sonarplus.kernel.transaction;

/**
 * Менеджер транзакций. Позволяет получить новую обычную транзакцию, либо транзакцию с точками сохранения.
 * Транзакция стартуется при получении.
 */
public interface TransactionManager {
    Transaction newTransaction() throws Exception;
    Transaction newTransactionWithSavepoint() throws Exception;

    // FIXME #45200 Удалить
    Transaction getTransactionById(int transactionId);
}