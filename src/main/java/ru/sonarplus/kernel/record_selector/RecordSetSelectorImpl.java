package ru.sonarplus.kernel.record_selector;

import javafx.util.Pair;
import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.SqlObjectExecutionService;
import ru.sonarplus.kernel.recordset.FieldValue;
import ru.sonarplus.kernel.recordset.RecordSet;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.CursorSpecification;
import ru.sonarplus.kernel.sqlobject.objects.FromContainer;
import ru.sonarplus.kernel.sqlobject.objects.PredicateComparison;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumnsContainer;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.ValueConstFactory;

import java.util.LinkedList;
import java.util.List;

import static ru.sonarplus.kernel.sqlobject.objects.Conditions.BooleanOp.AND;
import static ru.sonarplus.kernel.sqlobject.objects.PredicateComparison.ComparisonOperation.EQUAL;
import static ru.sonarplus.kernel.sqlobject.objects.QueryParam.ParamType.INPUT;

public class RecordSetSelectorImpl implements RecordSetSelector {


    private String[] columns;
    private String table;
    private final List<Pair<String, FieldValue>> keys = new LinkedList<>();
    private final SqlObjectExecutionService sqlObjectExecutionService;

    RecordSetSelectorImpl(SqlObjectExecutionService sqlObjectExecutionService) {
        this.sqlObjectExecutionService = sqlObjectExecutionService;
    }

    @Override
    public RecordSetSelector selectColumns(String... columns) {
        this.columns = columns;
        return this;
    }

    @Override
    public RecordSetSelector from(String table) {
        this.table = table;
        return this;
    }

    @Override
    public RecordSetSelector addKeyValuePair(String fieldName, FieldValue fieldValue) {
        Pair<String, FieldValue> pair = new Pair<>(fieldName, fieldValue);
        this.keys.add(pair);
        return this;
    }

    @Override
    public RecordSet select(ClientSession session) throws Exception {
        CursorSpecification cursorSpecification = new CursorSpecification()
                .setSelect(new Select()
                        .setColumns(buildColumns())
                        .setFrom(new FromContainer().addTable(table, ""))
                        .setWhere(buildConditions())
                );
        return sqlObjectExecutionService.executeCursor(session, cursorSpecification);
    }

    private Conditions buildConditions() {
        Conditions conditions = new Conditions(AND);
        for (Pair<String, FieldValue> pair : keys)
            conditions.addCondition(
                    new PredicateComparison(
                            new QualifiedRField("", pair.getKey()),
                            new QueryParam("", ValueConstFactory.ofFieldValue(pair.getValue()), INPUT),
                            EQUAL
                    )
            );
        return conditions;
    }

    private SelectedColumnsContainer buildColumns() {
        SelectedColumnsContainer selectedColumnsContainer = new SelectedColumnsContainer();
        for (String column : columns)
            selectedColumnsContainer.addColumn(new QualifiedRField("", column), "");
        return selectedColumnsContainer;
    }

}
