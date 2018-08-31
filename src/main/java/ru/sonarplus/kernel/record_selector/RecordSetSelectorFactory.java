package ru.sonarplus.kernel.record_selector;

import ru.sonarplus.kernel.SqlObjectExecutionService;

public class RecordSetSelectorFactory {
    public static RecordSetSelector newInstance(SqlObjectExecutionService sqlObjectExecutionService) {
        return new RecordSetSelectorImpl(sqlObjectExecutionService);
    }
}
