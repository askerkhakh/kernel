package ru.sonarplus.kernel.transaction;

public class TransactionImpl implements Transaction {

    private final TransactionManagerImpl transactionManager;
    private final DbTransaction dbTransaction;
    private final int id;

    TransactionImpl(TransactionManagerImpl transactionManager, DbTransaction dbTransaction, int id) throws Exception {
        this.transactionManager = transactionManager;
        this.id = id;
        this.dbTransaction = dbTransaction;
        this.dbTransaction.start();
    }

    @Override
    public void commit() throws Exception {
        transactionManager.commitTransaction(this);
        dbTransaction.commit();
    }

    @Override
    public void rollback() throws Exception {
        transactionManager.rollbackTransaction(this);
        dbTransaction.rollback();
    }

    @Override
    // FIXME #45200 Удалить
    public int getId() {
        return id;
    }

    DbTransaction getDbTransaction() {
        return dbTransaction;
    }
}
