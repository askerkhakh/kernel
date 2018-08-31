package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.requests.select;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.CTECycleClause;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderCTECycle extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        JSONArray jsonKeyColumns = Preconditions.checkNotNull(getArray(node, KEY_COLUMNS));
        Preconditions.checkState(jsonKeyColumns.size() != 0);

        String markerCycleColumn = (String) node.get(KEY_CTE_MARKER_CYCLE_COLUMN);
        Preconditions.checkState(!StringUtils.isEmpty(markerCycleColumn));

        String markerCycleValue = (String) node.get(KEY_CTE_MARKER_CYCLE_VALUE);
        Preconditions.checkState(!StringUtils.isEmpty(markerCycleValue));

        String markerCycleValueDefault = (String) node.get(KEY_CTE_MARKER_CYCLE_VALUE_DEFAULT);
        Preconditions.checkState(!StringUtils.isEmpty(markerCycleValueDefault));

        String columnPath = (String) node.get(KEY_CTE_COLUMN_PATH);

        CTECycleClause cycle = new CTECycleClause();
        cycle.columnPath = columnPath;
        cycle.markerCycleColumn = markerCycleColumn;
        cycle.markerCycleValue = markerCycleValue;
        cycle.markerCycleValueDefault = markerCycleValueDefault;

        String[] columns = new String[jsonKeyColumns.size()];
        for (int i = 0; i < jsonKeyColumns.size(); i++)
            columns[i] = (String)jsonKeyColumns.get(i);
        cycle.setColumns(columns);
        return cycle;
    }

    @Override
    public String getKind() { return KIND_CTE_CYCLE; }

}
