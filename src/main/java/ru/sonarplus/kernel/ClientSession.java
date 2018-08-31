package ru.sonarplus.kernel;

import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.transaction.Transaction;
import ru.sonarplus.kernel.transaction.TransactionManager;
import ru.sonarplus.kernel.transaction.TransactionManagerImpl;

import java.sql.Connection;

/**
 * ClientSession
 */
public class ClientSession {

    private final DbSchemaSpec dbSchemaSpec;
    private final Connection connection;
    private final User user;
    private final TransactionManager transactionManager;

    public ClientSession(DbSchemaSpec dbSchemaSpec, Connection connection, User user, SqlObjectExecutionService commandExecutionService) {
        this.dbSchemaSpec = dbSchemaSpec;
        this.connection = connection;
        this.user = user;
        transactionManager = new TransactionManagerImpl(this, commandExecutionService);
    }

    public DbSchemaSpec getDbSchemaSpec() {
        return dbSchemaSpec;
    }

    public Connection getConnection() {
        return connection;
    }

    public User getUser() {
        return user;
    }

    public Transaction newTransaction() throws Exception {
        return transactionManager.newTransaction();
    }

    public Transaction newTransactionWithSavepoint() throws Exception {
        return transactionManager.newTransactionWithSavepoint();
    }

    // FIXME 45200 Удалить
    public Transaction getTransactionById(int transactionId) {
        return transactionManager.getTransactionById(transactionId);
    }
}