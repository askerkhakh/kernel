package ru.sonarplus.kernel.data_change_service;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.DbSequenceService;
import ru.sonarplus.kernel.DbSettings;
import ru.sonarplus.kernel.SqlObjectExecutionService;
import ru.sonarplus.kernel.dbd_attributes.VirtualField;
import ru.sonarplus.kernel.dbschema.DbSchemaUtils;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.observer.Event;
import ru.sonarplus.kernel.observer.Observable;
import ru.sonarplus.kernel.observer.ObservableContainer;
import ru.sonarplus.kernel.observer.Observer;
import ru.sonarplus.kernel.observer.impl.EventManager;
import ru.sonarplus.kernel.recordset.Field;
import ru.sonarplus.kernel.recordset.FieldValue;
import ru.sonarplus.kernel.recordset.FieldValueFactory;
import ru.sonarplus.kernel.recordset.Record;
import ru.sonarplus.kernel.recordset.TableRecord;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.DMLFieldAssignment;
import ru.sonarplus.kernel.sqlobject.objects.DMLFieldsAssignments;
import ru.sonarplus.kernel.sqlobject.objects.DataChangeSqlQuery;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.PredicateComparison;
import ru.sonarplus.kernel.sqlobject.objects.PredicateIsNull;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryDelete;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryInsert;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryUpdate;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;
import static ru.sonarplus.kernel.sqlobject.objects.PredicateComparison.ComparisonOperation.EQUAL;
import static ru.sonarplus.kernel.sqlobject.objects.QueryParam.ParamType.INPUT;

/**
 * DataChangeServiceImpl
 */
@Named
@Singleton
public class DataChangeServiceImpl implements DataChangeService, ObservableContainer {

    private final Map<String, Observable> tableTriggersMap = new HashMap<>();

    // Observable, для рассылки событий "в себя"
    private final Observable observable = new EventManager();

    // Observable для "выливания воды" в случае, если нет Observable для таблицы
    private final Observable dummyObservable = new EventManager();

    private final SqlObjectExecutionService commandExecutionService;

    private final DbSequenceService dbSequenceService;

    private final DbSettings dbSettings;

    @Inject
    public DataChangeServiceImpl(SqlObjectExecutionService commandExecutionService,
                                 DbSequenceService dbSequenceService, DbSettings dbSettings) {
        this.commandExecutionService = commandExecutionService;
        this.dbSequenceService = dbSequenceService;
        this.dbSettings = dbSettings;
    }

    public Observable getObservable() {
        return observable;
    }

    private DataChangeSqlQuery buildQuery(DataChangeOperation operation, TableRecord record) {
        switch(operation) {
            case INSERT:
                SqlQueryInsert insert = new SqlQueryInsert();
                insert.table = record.getTableName();
                buildAssignments(record, insert.newAssignments());
                return insert;
            case MODIFY:
                SqlQueryUpdate update = new SqlQueryUpdate();
                update.table = record.getTableName();
                buildAssignments(record, update.newAssignments());
                update.setWhere(buildWhere(record));
                return update;
            case DELETE:
                SqlQueryDelete delete = new SqlQueryDelete();
                delete.table = record.getTableName();
                delete.setWhere(buildWhere(record));
                return delete;
            default:
                throw new AssertionError();
        }
    }

    private Conditions buildWhere(Record record) {
        Conditions conditions = new Conditions();
        for (Field field : record.getFields()) {
            if (!VirtualField.isFieldVirtual(verifyNotNull(field.getFieldSpec()))) {
                Predicate condition = buildFieldComparison(field);
                if (condition != null) {
                    conditions.addCondition(condition);
                }
            }
        }
        return conditions;
    }

    @Nullable
    private Predicate buildFieldComparison(Field field) {
        FieldValue oldFieldValue = requireNonNull(field.getOldValue());
        FieldTypeId type = oldFieldValue.getTypeId();
        if ((type == FieldTypeId.tid_BLOB) || (type == FieldTypeId.tid_MEMO)) {
            return null;
        }
        Object value = oldFieldValue.getValue();
        QualifiedRField qualifiedRField = new QualifiedRField("", field.getName());
        if (value == null) {
            if (dbSettings.useStandardNulls) {
                return new PredicateIsNull(null, qualifiedRField);
            }
            else
                return new PredicateComparison(
                        qualifiedRField,
                        new QueryParam(
                                null,
                                "",
                                new ValueConst(ValuesSupport.getZeroValue(type), type),
                                INPUT),
                        EQUAL);
        }
        else {
            return new PredicateComparison(
                    qualifiedRField,
                    new QueryParam(
                            null,
                            "",
                            new ValueConst(value, type),
                            INPUT),
                    EQUAL);
        }
    }

    private void buildAssignments(Record record, DMLFieldsAssignments assignments) {
        for (Field field : record.getFields()) {
            if (!VirtualField.isFieldVirtual(verifyNotNull(field.getFieldSpec()))) {
                DMLFieldAssignment fieldAssignment = new DMLFieldAssignment(assignments);
                fieldAssignment.setField(new QualifiedRField("", field.getName()));
                FieldValue fieldValue = requireNonNull(field.getValue());
                fieldAssignment.setExpr(
                        new QueryParam(
                                null,
                                "",
                                new ValueConst(fieldValue.getValue(), fieldValue.getTypeId()),
                                INPUT
                        )
                );
            }
        }
    }

    private boolean executeQuery(ClientSession session, DataChangeSqlQuery query) throws Exception {
        int rowsAffected = commandExecutionService.executeCommand(session, query);
        return (rowsAffected > 0);
    }

    @Nullable
    private Long setPrimaryKey(ClientSession session, TableRecord record) {
        TableSpec tableSpec = record.getTableSpec();
        FieldSpec primaryKeyFieldSpec = DbSchemaUtils.getPrimaryKeyFieldSpec(tableSpec);
        if ((primaryKeyFieldSpec != null) &&
                (primaryKeyFieldSpec.getDataTypeSpec().getFieldTypeId() == FieldTypeId.tid_LARGEINT) &&
                primaryKeyFieldSpec.getAutoAssignable()) {
            Field primaryKeyField = record.getFieldByName(primaryKeyFieldSpec.getFieldName());
            if (primaryKeyField.isNull()) {
                long seqValue = dbSequenceService.getNext(session, tableSpec.getName());
                primaryKeyField.setValue(FieldValueFactory.ofLong(seqValue));
                return seqValue;
            }
        }
        return null;
    }

    private void defaultApplyTriggers(Event event) throws Exception {
        ApplyTriggersEventParameters parameters = (ApplyTriggersEventParameters) event.getParameters();
        if (parameters.operation == DataChangeOperation.INSERT) {
            parameters.calculatedPrimaryKey = setPrimaryKey(parameters.session, parameters.record);
        }
        parameters.succsess = executeQuery(parameters.session, buildQuery(parameters.operation, parameters.record));
    }

    private void defaultCommitDataChange(Event event) throws Exception {
        CommitDataChangeEventParameters commitDataChangeEventParameters = (CommitDataChangeEventParameters) event.getParameters();
        Observable tableTriggerObservable = tableTriggersMap.get(commitDataChangeEventParameters.record.getTableName());
        tableTriggerObservable = (tableTriggerObservable != null) ? tableTriggerObservable : dummyObservable;

        ApplyTriggersEventParameters applyTriggersEventParameters = new ApplyTriggersEventParameters();
        applyTriggersEventParameters.session = commitDataChangeEventParameters.session;
        applyTriggersEventParameters.operation = commitDataChangeEventParameters.operation;
        applyTriggersEventParameters.record = commitDataChangeEventParameters.record;

        tableTriggerObservable.SendEvent(applyTriggersEventParameters, this::defaultApplyTriggers);

        commitDataChangeEventParameters.calculatedPrimaryKey = applyTriggersEventParameters.calculatedPrimaryKey;
        commitDataChangeEventParameters.succsess = applyTriggersEventParameters.succsess;
    }

    @Override
    public Long insertRecord(ClientSession session, TableRecord row, Map<String, String> options) throws Exception {
        CommitDataChangeEventParameters parameters = new CommitDataChangeEventParameters();
        parameters.session = session;
        parameters.operation = DataChangeOperation.INSERT;
        parameters.record = row;
        parameters.options = options;
        this.getObservable().SendEvent(parameters, this::defaultCommitDataChange);
        return parameters.calculatedPrimaryKey;
    }

    @Override
    public boolean modifyRecord(ClientSession session, TableRecord row, Map<String, String> options) throws Exception {
        CommitDataChangeEventParameters parameters = new CommitDataChangeEventParameters();
        parameters.session = session;
        parameters.operation = DataChangeOperation.MODIFY;
        parameters.record = row;
        parameters.options = options;
        this.getObservable().SendEvent(parameters, this::defaultCommitDataChange);
        return parameters.succsess;
    }

    @Override
    public boolean deleteRecord(ClientSession session, TableRecord row, Map<String, String> options) throws Exception {
        CommitDataChangeEventParameters parameters = new CommitDataChangeEventParameters();
        parameters.session = session;
        parameters.operation = DataChangeOperation.DELETE;
        parameters.record = row;
        parameters.options = options;
        this.getObservable().SendEvent(parameters, this::defaultCommitDataChange);
        return parameters.succsess;
    }

    public void registerGeneralTrigger(Observer trigger) {
        this.getObservable().installEventHandler(CommitDataChangeEventParameters.class, trigger);
    }

    public void registerTableTrigger(String tableName, Observer tableTrigger) {
        Observable observable = tableTriggersMap.get(tableName);
        if (observable == null) {
            observable = new EventManager();
            tableTriggersMap.put(tableName, observable);
        }
        observable.installEventHandler(ApplyTriggersEventParameters.class, tableTrigger);
    }

}