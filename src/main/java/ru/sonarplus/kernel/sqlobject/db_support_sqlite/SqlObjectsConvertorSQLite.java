package ru.sonarplus.kernel.sqlobject.db_support_sqlite;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class SqlObjectsConvertorSQLite extends SqlObjectsConvertor {

    public SqlObjectsConvertorSQLite() { super(true); }

    @Override
    protected SqlObjectsDbSupportUtils createDBSupport() { return new SqlObjectsDBSupportUtilsSQLite(); }

    @Override
    protected String convertOrderByItem(OrderByItem item, ConvertParams params, ConvertState state)
            throws Exception {
        String expr = convert(item.getExpr(), params, state);
        StringBuilder sb = new StringBuilder(expr);
        switch (item.nullOrdering) {
            case NULLS_FIRST:
                sb.append(" IS NOT NULL, ");
                sb.append(expr);
                break;

            case NULLS_LAST:
                sb.append(" IS NULL, ");
                sb.append(expr);
                break;

            default:
                break;
        }
        if (item.direction != OrderByItem.OrderDirection.ASC) {
            sb.append(' ');
            sb.append(orderDirectionToStr(item.direction));
        }
        return sb.toString();
    }

    @Override
    protected String convertPredicateInSelect(PredicateInQuery item, ConvertParams params, ConvertState state)
            throws Exception{
        Preconditions.checkState(item.getTuple().itemsCount() != 0, "Пустой кортеж в условии in select");
        if (item.getTuple().itemsCount() == 1)
            // условие по одному полю - конвертируем стандартно
            return super.convertPredicateInSelect(item, params, state);
        else
            // по более, чем одному полю условие "in select" в SQLite не работает. обернём в Exists
            return SQLite_convertPredicateInSelectToExists(item, params, state);
    }

    protected String SQLite_convertPredicateInSelectToExists(PredicateInQuery item, ConvertParams params, ConvertState state)
            throws Exception{
        /* (field1, .. fieldN) in (select <c1>, .. <cn>...) =>
            Exists(
                    select 1 from (select <c1> c1, .. <cn> cn...)
                    where field1 = c1 and ... fieldN = cN
            ) */
        Select select = item.findSelect();
        Preconditions.checkNotNull(select);

        final String SUB_REQUEST_PREFIX = "sub_";
        String inSelectAlias;
        inSelectAlias = SqlObjectUtils.getRequestTableAlias(select);
        if (StringUtils.isEmpty(inSelectAlias))
            inSelectAlias = SqlObjectUtils.getRequestTableName(select);
        Preconditions.checkState(!StringUtils.isEmpty(inSelectAlias));
        inSelectAlias = SUB_REQUEST_PREFIX + inSelectAlias;

        Preconditions.checkState(item.getTuple().itemsCount() == select.getColumns().itemsCount());
        Select backSelect = (Select)select.clone();
        try {
            Select selectOne = new Select();
            item.setSelect(selectOne);
            selectOne.newColumns()
                    .addColumn(new Expression("1", true), "");
            selectOne.newFrom()
                    .addQuery(select, inSelectAlias, null);
            final String COLUMN_PREFIX = "c_";
            int i = 0;
            String[] columns = new String[select.getColumns().itemsCount()];
            for (SqlObject child: select.getColumns()) {
                columns[i] = COLUMN_PREFIX + String.valueOf(i);
                ((SelectedColumn) child).alias = columns[i];
                i++;
            }
            StringBuilder sb = new StringBuilder();
            if (item.not)
                sb.append("NOT ");
            sb.append("EXISTS(");
                sb.append(convert(selectOne, params, state));
                sb.append(" WHERE ");
                i = 0;
                for (SqlObject tupleItem: item.getTuple()) {
                    if (i != 0)
                        sb.append(" AND ");
                    sb.append(convert(tupleItem, params, state));
                    sb.append('=');
                    sb.append(inSelectAlias);
                    sb.append('.');
                    sb.append(columns[i]);
                    i++;
                }
            sb.append(')');
            return sb.toString();
        }
        finally {
            item.setSelect(backSelect);
        }
    }

    @Override
    protected String convertUnionItem(UnionItem item, ConvertParams params, ConvertState state)
            throws Exception {
        Select unitedSelect = item.getSelect();
        // скобки для union-ов в SQLite не поддержаны,
        // поэтому просто контракт на их отсутствие.
        //   признаком наличия скобок в объединении формально считаем наличие у присоединяемого
        //   запроса своих объединений
        Preconditions.checkState(!(unitedSelect.findUnions() != null && unitedSelect.findUnions().isHasChilds()));
        StringBuilder sb = new StringBuilder();
        sb.append(' ');
        sb.append(unionTypeToString(item.unionType).toUpperCase());
        sb.append(' ');
        sb.append(convert(unitedSelect, params, state));
        return sb.toString();
    }

}
