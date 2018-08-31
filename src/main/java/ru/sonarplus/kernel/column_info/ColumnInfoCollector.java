package ru.sonarplus.kernel.column_info;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.CursorSpecification;
import ru.sonarplus.kernel.sqlobject.objects.Expression;
import ru.sonarplus.kernel.sqlobject.objects.FromClauseItem;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedField;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumn;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumnsContainer;
import ru.sonarplus.kernel.sqlobject.objects.Source;
import ru.sonarplus.kernel.sqlobject.objects.SourceQuery;
import ru.sonarplus.kernel.sqlobject.objects.SourceTable;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Собирает информацию о колонках по SqlObject-запросу
 */
public class ColumnInfoCollector {

    public static ColumnInfo[] collect(CursorSpecification cursorSpecification, DbSchemaSpec dbSchemaSpec,
                                       SqlObjectsConvertor converter) {
        return collect(cursorSpecification.getSelect(), dbSchemaSpec, converter);
    }

    private static ColumnInfo[] collect(Select select, DbSchemaSpec dbSchemaSpec, SqlObjectsConvertor converter) {
        List<ColumnInfo> columnsInfo = new ArrayList<>();

        SelectedColumnsContainer columns = Preconditions.checkNotNull(select).getColumns();

        // если единственная колонка является настоящей звёздочкой, должен быть подзапрос с колонками
        if (columns.itemsCount() == 1 && SqlObjectUtils.isPureAsterisk(columns.getItem(0)))
            return collect(((SourceQuery)select.getBaseSource()).getSelect(), dbSchemaSpec, converter);

        for (SqlObject item: columns)
            columnsInfo.add(columnInfoByColumn(select, (SelectedColumn) item, dbSchemaSpec, converter));

        return columnsInfo.toArray(new ColumnInfo[0]);
    }

    private static ColumnInfo columnInfoByColumn(Select select, SelectedColumn column, DbSchemaSpec dbSchemaSpec,
                                                 SqlObjectsConvertor converter) {
        ColumnInfoImpl columnInfo = new ColumnInfoImpl();
        fillColumnInfoForColumn(dbSchemaSpec, select, column, columnInfo, true, converter);
        return columnInfo;
    }

    private static void fillColumnInfoForColumn(
            DbSchemaSpec dbSchemaSpec,
            Select select,
            SelectedColumn column,
            ColumnInfoImpl columnInfo,
            boolean fillAllInfo,
            @Nullable SqlObjectsConvertor converter) {
        ColumnExpression expr = column.getColExpr();
        Preconditions.checkState(expr != null);
        Class exprClass = expr.getClass();

        // в исполняемом запросе не должно быть недистиллированных элементов
        Preconditions.checkState(exprClass != QualifiedRField.class);
        if (exprClass == Expression.class)
            Preconditions.checkState(((Expression)expr).isPureSql);

        if (exprClass == QualifiedField.class || exprClass == Expression.class) {
            ColumnExprTechInfo techInfo = expr.distTechInfo;
            // блок techInfo не будет заполнен в том случае, если:
            // - запрос, содержащий данную колонку, не подвергался дистилляции;
            // - или исходное выражение колонки было Expression, для которого techInfo создаётся формально;
            // - или исходное выражение колонки было QualifiedField, а не Qualified_R_Field. в этом случае в techInfo
            //   будет заполнен только nativeFieldName;
            boolean tech_info_prepared = techInfo != null && !StringUtils.isEmpty(techInfo.originTableName);

            // есть тех.информация - воспользуемся ей, чтобы не бегать рекурсивно по запросу
            if (tech_info_prepared) {
                fillColumnInfoForColumnByTechInfo(columnInfo, techInfo);
                if (fillAllInfo)
                    if (!StringUtils.isEmpty(column.alias))
                        columnInfo.setFieldName(column.alias);
                    else if (columnInfo.getFieldSpec() != null)
                        columnInfo.setFieldName(getColumnAliasUsingTechInfo(SqlObjectUtils.getRequestTableName(select), techInfo));
                    else
                        columnInfo.setFieldName(getColumnAliasByExpr(column.getColExpr(), requireNonNull(converter)));
            }
            // тех.информации нет - придётся бегать по дереву запроса.
            // как собирать информацию о колонке - понятно только для QualifiedField
            else if (exprClass == QualifiedField.class) {
                fillColumnInfoForColumnBySelect(dbSchemaSpec, select, (QualifiedField) expr, columnInfo);
                if (fillAllInfo)
                    if (!StringUtils.isEmpty(column.alias))
                        columnInfo.setFieldName(column.alias);
                    else if (columnInfo.getFieldSpec() != null)
                        columnInfo.setFieldName(
                                getColumnAliasNoUsingTechInfo(select, column, columnInfo.getFieldSpec(), requireNonNull(converter))
                        );
                    else
                        columnInfo.setFieldName(getColumnAliasByExpr(column.getColExpr(), requireNonNull(converter)));
            }
        }

        if (fillAllInfo && StringUtils.isEmpty(columnInfo.getFieldName()) && !StringUtils.isEmpty(column.alias))
            columnInfo.setFieldName(column.alias);
    }

    private static String getColumnAliasNoUsingTechInfo(Select select, SelectedColumn column, FieldSpec fieldSpec,
                                                        SqlObjectsConvertor converter) {
        ColumnExpression expr = column.getColExpr();
        if (expr.getClass() == QualifiedField.class) {
            QualifiedField qfield = (QualifiedField) expr;
            FromClauseItem from = Preconditions.checkNotNull(select.getFrom().findItem(qfield.alias));
            if (!StringUtils.isEmpty(from.getAlias()))
                return QualifiedName.formQualifiedNameString(from.getAlias(), fieldSpec.getFieldName());

            if (select.getFrom().firstSubItem() != from)
                return QualifiedName.formQualifiedNameString(from.getTableName(), fieldSpec.getFieldName());

            return fieldSpec.getFieldName();
        }
        else if (expr.getClass() == Expression.class) {
            // в качестве "имени" колонки используем строку выражения
            return Preconditions.checkNotNull(converter).convert(expr);
        }
        return "";
    }

    private static String getColumnAliasUsingTechInfo(String mainTable, ColumnExprTechInfo techInfo) {
        // имя табличного выражения (алиас) не совпадает с именем таблицы - вернём квалифицированное им имя поля
        if (!StringUtils.equals(techInfo.originTableName, techInfo.tableExprName))
            return QualifiedName.formQualifiedNameString(techInfo.tableExprName, techInfo.dbdFieldName);

        // имя табличного выражения совпадает с именем таблицы:
        // ...если табличное выражение соответствует основной таблице запроса - вернём просто имя поля
        if (StringUtils.equals(mainTable, techInfo.originTableName))
            return techInfo.dbdFieldName;

        // ...подлитая таблица - вернём квалифицированное имя
        return QualifiedName.formQualifiedNameString(techInfo.tableExprName, techInfo.dbdFieldName);
    }

    private static String getColumnAliasByExpr(ColumnExpression expr, SqlObjectsConvertor converter) {
        if (expr.getClass() == QualifiedField.class)
            return ((QualifiedField)expr).getQName().qualifiedNameToString();
        else if (expr.getClass() == Expression.class)
            return converter.convert(expr);
        else
            throw new SqlObjectException(expr.getClass().getSimpleName());
    }

    private static SelectedColumn findColumnByName(SelectedColumnsContainer columns, String name)
            throws SqlObjectException{
        for (SqlObject item: columns) {
            SelectedColumn column = (SelectedColumn)item ;
            String columnName;
            if (!StringUtils.isEmpty(column.alias))
                columnName = column.alias;
            else {
                ColumnExpression expr = Preconditions.checkNotNull(column.getColExpr());
                if (expr.getClass() == QualifiedField.class)
                    columnName = ((QualifiedField)expr).fieldName;
                else
                    continue;
            }
            if (StringUtils.equals(name, columnName))
                return column;
        }
        final String COLUMN_NOT_FOUND = "Колонка '%s' не найдена";
        throw new SqlObjectException(String.format(COLUMN_NOT_FOUND, String.valueOf(name)));
    }

    private static void fillColumnInfoForColumnBySelect(
            DbSchemaSpec dbSchemaSpec,
            Select select,
            QualifiedField qfield,
            ColumnInfoImpl columnInfo) {
        FromClauseItem from = Preconditions.checkNotNull(select.getFrom().findItem(qfield.alias));
        Source source = Preconditions.checkNotNull(from.getTableExpr().getSource());
        if (source.getClass() == SourceTable.class) {
            String tableName = source.getTable();
            TableSpec tableSpec = dbSchemaSpec.findTableSpec(tableName);
            if (tableSpec != null) {
                columnInfo.setFieldSpec(findFieldSpecByLatinName(tableSpec, qfield.fieldName));
            }
            // else;
            // на самом деле отсутствующая в описателе таблица не говорит ни о чём. это может быть CTE-запрос.
            // но в Delphi эту ситуацию не анализировали, так что не будем и здесь. Пока ...
        }
        else if (source.getClass() == SourceQuery.class) {
            Select localSelect = ((SourceQuery)source).getSelect();
            fillColumnInfoForColumn(dbSchemaSpec, localSelect, findColumnByName(localSelect.getColumns(), qfield.fieldName),
                    columnInfo, false, null);
        }
        else
            throw new SqlObjectException(source.getClass().getSimpleName());
    }

    private static void fillColumnInfoForColumnByTechInfo(ColumnInfoImpl columnInfo, ColumnExprTechInfo techInfo) {
        columnInfo.setFieldSpec(techInfo.fieldSpec);
    }

    @Nullable
    private static FieldSpec findFieldSpecByLatinName(TableSpec tableSpec, String fieldName) {
        for (FieldSpec fieldSpec: tableSpec.items)
            if (StringUtils.equalsIgnoreCase(fieldSpec.getLatinName(), fieldName))
                return fieldSpec;
        return null;
    }

 }