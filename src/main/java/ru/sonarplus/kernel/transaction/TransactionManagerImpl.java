package ru.sonarplus.kernel.transaction;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.SqlObjectExecutionService;

import java.util.Stack;

import static com.google.common.base.Verify.verify;

/**
 * Менеджер транзакций - обеспечивает контроль правильного использования транзакций
 * (отлавливает различные ошибочные ситуации, такие как вызов commit после rollback и т.п.)
 */
public class TransactionManagerImpl implements TransactionManager {

    private final ClientSession session;
    private final SqlObjectExecutionService commandExecutionService;
    private final Stack<Transaction> transactionStack = new Stack<>();
    private boolean rollbackTriggered;

    public TransactionManagerImpl(ClientSession session, SqlObjectExecutionService commandExecutionService) {
        this.session = session;
        this.commandExecutionService = commandExecutionService;
    }

    private void verifyRollbackState() {
        verify(!rollbackTriggered, "Попытка старта вложенной транзакции, после отката другой транзакции, в рамках этой же охватывающей");
    }

    @Override
    public Transaction newTransaction() throws Exception {
        verifyRollbackState();
        return newTransactionImpl(transactionStack.empty() ? new SimpleDbTransaction(session, commandExecutionService) : new DummyDbTransaction());
    }

    @Override
    public Transaction newTransactionWithSavepoint() throws Exception {
        verifyRollbackState();
        return newTransactionImpl(new SavepointDbTransaction(session, commandExecutionService, transactionStack.size()));
    }

    @Override
    // FIXME #45200 Удалить
    public Transaction getTransactionById(int transactionId) {
        return transactionStack.get(transactionId);
    }

    private Transaction newTransactionImpl(DbTransaction dbTransaction) throws Exception {
        Transaction transaction = new TransactionImpl(this, dbTransaction, transactionStack.size());
        transactionStack.push(transaction);
        return transaction;
    }

    private void popTransaction(TransactionImpl transaction) {
        Transaction poppedTransaction = transactionStack.pop();
        verify(poppedTransaction == transaction, "Нарушен порядок завершения вложенных транзакций");
    }

    void commitTransaction(TransactionImpl transaction) {
        popTransaction(transaction);
        verify(!rollbackTriggered, "Попытка совершить успешное завершение охватывающей транзакции, после отката вложенной");
    }

    void rollbackTransaction(TransactionImpl transaction) {
        popTransaction(transaction);
        if (transactionStack.empty()) {
            rollbackTriggered = false;
        }
        else {
            if (!(transaction.getDbTransaction() instanceof SavepointDbTransaction)) {
                rollbackTriggered = true;
            }
        }
    }
}
