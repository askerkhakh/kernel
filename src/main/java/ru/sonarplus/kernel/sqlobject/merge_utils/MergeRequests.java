package ru.sonarplus.kernel.sqlobject.merge_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.RenamingDict;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MergeRequests {

    private Conditions.BooleanOp booleanOp;
    private Select[] mergingSelects;
    private RenamingIndex renamingIndex;
    private Renamer renamer;

    private static final String FIRST_FROM_ITEM_MUST_BE_TABLE = "Первый элемент раздела FROM участвующего в объединении запроса должен быть таблицей";
    private static final String BASE_TABLES_MUST_BE_SAME = "Основные таблицы объединяемых запросов должны быть одинаковыми";
    private static final String GROUP_BY_CLAUSE_MUST_BE_IN_LEAD_REQ_ONLY = "Раздел GROUP BY может быть в только в первом из объединяемых запросов";
    private static final String UNIONS_NOT_SUPPORTED = "Объединение запросов, содержащих UNIONs не поддержано";

    public MergeRequests() {

    }

    public CursorSpecification merge(Conditions.BooleanOp booleanOp, List<RenamingDict> dictList, SqlQuery...requests)
            throws CloneNotSupportedException, SqlObjectException {
        this.booleanOp = booleanOp;
        this.mergingSelects = getNotNullSelects(requests);
        if (mergingSelects.length == 0)
            return null;
        if (this.booleanOp == Conditions.BooleanOp.OR && mergingSelects.length > 1)
            for(Select select: mergingSelects)
                JoinsConvertor.convertInnerJoinsToLeftJoins(select);

        this.renamingIndex = new RenamingIndex().build(mergingSelects);
        this.renamer = new Renamer(renamingIndex);
        Select mergedSelect = internalMerge();
        CursorSpecification cursor = internalMergeOrderBy(mergedSelect, mergingSelects[0], requests[0]);
        cursor.setFetchFirst(getFetchFirst(requests));
        if (dictList != null) {
            dictList.clear();
            int notNullSelectIndex = 0;
            for(SqlQuery request: requests) {
                if (request != null) {
                    dictList.add(renamingIndex.getTablesRenamingFor(mergingSelects[notNullSelectIndex]));
                    notNullSelectIndex++;
                }
                else
                    dictList.add(null);
            }
        }
        return cursor;
    }

    protected long getFetchFirst(SqlQuery...requests) {
        long result = 0;
        for(SqlQuery request: requests)
            if (request != null && request instanceof CursorSpecification) {
                CursorSpecification cursor = (CursorSpecification) request;
                if ((cursor.getFetchFirst() != 0) && (result == 0 || result > cursor.getFetchFirst()))
                        result = cursor.getFetchFirst();
            }
        return result;
    }

    protected static class Renamer {
        private RenamingIndex renamingIndex;

        public Renamer(RenamingIndex renamingIndex) {
            this.renamingIndex = renamingIndex;
        }

        public void rename(SqlObject root, Select indexedSelect) {
            if (root instanceof Parameter) {
                Parameter param = (Parameter) root;
                param.parameterName = renamingIndex.getParamsRenamingFor(indexedSelect).rename(param.parameterName);
            }
            else if (root instanceof FromClauseItem) {
                FromClauseItem from = (FromClauseItem) root;
                if (from.getTableExpr().getSource() instanceof SourceTable)
                  renameFromTable((FromClauseItem) root, indexedSelect);
                else
                    from.setAlias(renamingIndex.getTablesRenamingFor(indexedSelect).rename(from.getAliasOrName()));
            }
            else if (root instanceof CommonTableExpression) {
                CommonTableExpression cte = (CommonTableExpression) root;
                cte.alias = renamingIndex.getCTEsRenamingFor(indexedSelect).rename(cte.alias);
            }
            else if (root instanceof QualifiedField) {
                QualifiedField qfield = (QualifiedField) root;
                qfield.alias = renamingIndex.getTablesRenamingFor(indexedSelect).rename(qfield.alias);
            }
            else if (root instanceof Expression) {
                Expression expr = (Expression) root;
                expr.setExpr(
                        ExprUtils.exprReplaceAliasesForQNames(
                                expr.getExpr(),
                                renamingIndex.getTablesRenamingFor(indexedSelect)));
            }
            for(SqlObject item: root)
                rename(item, indexedSelect);
        }

        public void renameFromTable(FromClauseItem from, Select indexedSelect) {
            SourceTable source = (SourceTable)from.getTableExpr().getSource();
            String newName = renamingIndex.getCTEsRenamingFor(indexedSelect).rename(source.getTable());
            String newAlias = renamingIndex.getTablesRenamingFor(indexedSelect).rename(from.getAliasOrName());
            if (newName.compareToIgnoreCase(newAlias) == 0)
                //если алиас таблицы совпадает с именем таблицы - зачем нам алиас?
                newAlias = "";
            source.setTable(newName);
            from.setAlias(newAlias);
        }
    }

    protected Select internalMerge()
            throws SqlObjectException {
        SelectsCheckResult checkResult = checkSelects();
        if (checkResult.onceSelect != null)
        {
            // если задан только один запрос - вернём его копию
            renamer.rename(checkResult.onceSelect, checkResult.onceSelect);
            return checkResult.onceSelect;
        }

        if (checkResult.isHasGroupBy)
            throw new MergeException(GROUP_BY_CLAUSE_MUST_BE_IN_LEAD_REQ_ONLY);

        Select target = new Select();
        Conditions newWhere = target.newWhere();
        newWhere.booleanOp = this.booleanOp;
        newWhere.not = false;

        // собрали параметры
        mergeParameters(target);
        // собрали разделы WITH
        mergeWiths(target);
        // источники записей (FROM)
        mergeFroms(target);
        // условия (WHERE)
        mergeWheres(target);
        // группировки (GROUP BY)
        mergeGroupBy(target);
        // объединения
        mergeUnions(target);
        // колонки
        mergeColumns(target);
        //<oracle>
        mergeOraFtsMarkers(target);
        //</oracle>

        return target;
    }

    protected void mergeParameters(Select target)
            throws SqlObjectException {
        // собираем параметры из запросов, выполняя их переименования
        for (Select select: this.mergingSelects) {
            QueryParams params = select.getParams();
            if (params != null)
                while (params.isHasChilds()) {
                    QueryParam param = (QueryParam) params.firstSubItem();
                    renamer.rename(param, select);
                    target.newParams().insertItem(param);
                }
        }
    }

    protected void mergeWiths(Select target)
            throws SqlObjectException {
        // собираем разделы WITH из запросов, выполняя требуемые переименования
        for(Select select: this.mergingSelects) {
            CTEsContainer with = select.findWith();
            if (with != null)
                while (with.isHasChilds()) {
                    CommonTableExpression cte = (CommonTableExpression) with.firstSubItem();
                    renamer.rename(cte, select);
                    target.newWith().insertItem(cte);
                }
        }
    }

    protected void mergeFroms(Select target)
            throws SqlObjectException {
        // собираем FROM'ы из запросов, выполняя их переименования
        for(Select select: this.mergingSelects) {
            while(select.getFrom().isHasChilds()) {
                FromClauseItem from = (FromClauseItem) select.getFrom().firstSubItem();
                if (renamingIndex.isJoiningPathDuplicates(from, select))
                    from.getOwner().removeItem(from);
                else {
                    if (from.getTableExpr().getSource() instanceof SourceQuery) {
                        from.setAlias(renamingIndex.getTablesRenamingFor(select).rename(from.getAliasOrName()));
                        renamer.rename(((SourceQuery) from.getTableExpr().getSource()).getSelect(), select);
                    }
                    else if (from.getTableExpr().getSource() instanceof SourceTable)
                        renamer.renameFromTable(from, select);
                    else
                        Preconditions.checkState(false);

                    Join join = from.getJoin();
                    if (join != null)
                        renamer.rename(join, select);
                    target.newFrom().insertItem(from);
                }
            }
        }
    }

    protected void mergeWheres(Select target)
            throws SqlObjectException {
        // собираем разделы WHERE из запросов, выполняя требуемые переименования
        for(Select select: this.mergingSelects) {
            Conditions where = select.findWhere();
            if (where != null && !where.isEmpty()) {
                renamer.rename(where, select);
                target.newWhere().insertItem(where);
            }
        }
    }

    protected void mergeGroupBy(Select target)
            throws SqlObjectException {
        for(Select select: this.mergingSelects) {
            GroupBy groupBy = select.findGroupBy();
            if (groupBy != null && groupBy.isHasChilds()) {
                renamer.rename(groupBy, select);
                target.setGroupBy(groupBy);
                // только из первого запроса
                break;
            }
        }
    }

    protected void mergeUnions(Select target) throws MergeException{
        // TODO Нужно будет понять, каким образом можно объединять запросы, содержащие union-ы
        for(Select select: this.mergingSelects)
            if (select.findUnions() != null)
                throw new MergeException(UNIONS_NOT_SUPPORTED);
    }

    protected boolean columnsContainsColumn(SelectedColumnsContainer columns, SelectedColumn column) {
        if (columns == null)
            return false;
        // здесь не проверяются дублирующиеся выражения. может быть следовало бы...
        if (StringUtils.isEmpty(column.alias) && column.getColExpr() instanceof QualifiedField) {
            QualifiedName qname = ((QualifiedField)column.getColExpr()).getQName();
            /*
                Из DSP:
                1. Если у колонки есть алиас, то поле в датасете будет называться этим алиасом и, тот кто
                добавил эту колонку с алиасом на это рассчитывает. Т.е. если есть две одинаковые колонки, но
                одна с алиасом, другая - без, то в датасете должны быть оба этих поля.
                2. Если колонка не является TQualifiedField, то сравнить с другими её будет сложно, поэтому считаем,
                    что она отличается от всех колонок.
            */
            for(SqlObject item: columns) {
                SelectedColumn containedColumn = (SelectedColumn) item;
                if (StringUtils.isEmpty(containedColumn.alias) && containedColumn.getColExpr() instanceof QualifiedField) {
                    QualifiedField qfield = (QualifiedField) containedColumn.getColExpr();
                    if (qname.alias.equals(qfield.alias) && qname.name.equals(qfield.fieldName))
                        return true;
                }
            }

        }
        return false;
    }

    protected void mergeColumns(Select target)
            throws SqlObjectException {
        boolean isFirstSelect = true;
        for (Select select: this.mergingSelects) {
            SelectedColumnsContainer columns = select.findColumns();
            if (columns != null) {
                renamer.rename(columns, select);
                for (SqlObject child: columns) {
                    SelectedColumn column = (SelectedColumn) child;
                    // звёздочку добавляем в список только если она в первом запросе
                    if (!columnsContainsColumn(target.findColumns(), column) && (!SqlObjectUtils.isAsterisk(column.getColExpr()) || isFirstSelect))
                        target.newColumns().insertItem(column);
                }
            }
            isFirstSelect = false;
        }
    }

    protected void mergeOraFtsMarkers(Select target) {
        for (Select select: this.mergingSelects)
            target.techInfo.addOraFtsMarkers(select.techInfo.oraFTSMarkers);
    }

    protected CursorSpecification internalMergeOrderBy(Select merged, Select firstMerging, SqlQuery firstSource)
            throws CloneNotSupportedException, SqlObjectException {
        CursorSpecification cursor = merged.toCursorSpecification(false);
        OrderBy orderBy = firstSource instanceof CursorSpecification ? ((CursorSpecification) firstSource).findOrderBy() : null;
        if (orderBy != null) {
            OrderBy orderByClone = (OrderBy) orderBy.clone();
            renamer.rename(orderByClone, firstMerging);
            cursor.setOrderBy(orderByClone);
        }
        return cursor;
    }

    protected class SelectsCheckResult {
        public boolean isHasGroupBy;
        public Select onceSelect;
    }

    protected SelectsCheckResult checkSelects()
            throws SqlObjectException {
        SelectsCheckResult result = new SelectsCheckResult();
        FromClauseItem firstFrom;
        String baseTable = "";
        boolean isFirstSelect = true;
        for(Select item: mergingSelects){
            // первый элемент раздела from должен быть таблицей
            firstFrom = (FromClauseItem) item.getFrom().firstSubItem();
            if (!(
                    firstFrom.getTableExpr().getSource() instanceof SourceTable &&
                    SqlObjectUtils.CTE.findCTE(firstFrom) == null))
                throw new MergeException(FIRST_FROM_ITEM_MUST_BE_TABLE);
            if (isFirstSelect)
                baseTable = SqlObjectUtils.getRequestTableName(item);
            else {
                if (baseTable.compareToIgnoreCase(SqlObjectUtils.getRequestTableName(item)) != 0)
                    throw new MergeException(BASE_TABLES_MUST_BE_SAME);
                // наличие группировок не интересует только для первого запроса, для последующих - ситуацию обработаем
                GroupBy groupBy = item.findGroupBy();
                result.isHasGroupBy = result.isHasGroupBy || (groupBy != null && groupBy.isHasChilds());
            }
            isFirstSelect = false;
        }
        if (mergingSelects.length == 1)
            result.onceSelect = mergingSelects[0];

        return result;
    }

    protected Select[] getNotNullSelects(SqlQuery...requests)
            throws CloneNotSupportedException, SqlObjectException {
        List<Select> result = new ArrayList<>();
        for(SqlQuery request: requests)
            if (request != null)
                result.add(cloneSelect(checkRequest(request)));

        return result.toArray(new Select[0]);
    }

    protected SqlQuery checkRequest(SqlQuery request) throws MergeException{
        if (!(request instanceof Select) && !(request instanceof CursorSpecification))
            throw new MergeException(String.format(
                    "Неверный тип (%s) запроса. Ожидается тип %s или %s",
                    request.getClass().getName(),
                    Select.class.getName(), CursorSpecification.class.getName()));
        return request;
    }

    protected Select cloneSelect(SqlQuery request)
            throws CloneNotSupportedException, SqlObjectException {
        if (request instanceof Select)
            return (Select) request.clone();
        else if (request instanceof CursorSpecification)
            return ((CursorSpecification) request).toSelect(true);
        return null;
    }
}
