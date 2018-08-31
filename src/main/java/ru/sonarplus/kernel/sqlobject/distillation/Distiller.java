package ru.sonarplus.kernel.sqlobject.distillation;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.db_support.FullTextEngine;
import ru.sonarplus.kernel.sqlobject.db_support.FunctionsReplacer;
import ru.sonarplus.kernel.sqlobject.db_support.PredicateForCodeComparisonTranslator;
import ru.sonarplus.kernel.sqlobject.distillation_old.Utils;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.objects.CTEsContainer;
import ru.sonarplus.kernel.sqlobject.objects.CallStoredProcedure;
import ru.sonarplus.kernel.sqlobject.objects.CaseSearch;
import ru.sonarplus.kernel.sqlobject.objects.CaseSimple;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.CommonTableExpression;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.CursorSpecification;
import ru.sonarplus.kernel.sqlobject.objects.DMLFieldAssignment;
import ru.sonarplus.kernel.sqlobject.objects.DMLFieldsAssignments;
import ru.sonarplus.kernel.sqlobject.objects.Expression;
import ru.sonarplus.kernel.sqlobject.objects.FromClauseItem;
import ru.sonarplus.kernel.sqlobject.objects.FromContainer;
import ru.sonarplus.kernel.sqlobject.objects.GroupBy;
import ru.sonarplus.kernel.sqlobject.objects.Join;
import ru.sonarplus.kernel.sqlobject.objects.OraFTSMarker;
import ru.sonarplus.kernel.sqlobject.objects.OraFTSRange;
import ru.sonarplus.kernel.sqlobject.objects.OrderBy;
import ru.sonarplus.kernel.sqlobject.objects.OrderByItem;
import ru.sonarplus.kernel.sqlobject.objects.ParamRef;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.PredicateBetween;
import ru.sonarplus.kernel.sqlobject.objects.PredicateComparison;
import ru.sonarplus.kernel.sqlobject.objects.PredicateExists;
import ru.sonarplus.kernel.sqlobject.objects.PredicateForCodeComparison;
import ru.sonarplus.kernel.sqlobject.objects.PredicateFullTextSearch;
import ru.sonarplus.kernel.sqlobject.objects.PredicateInQuery;
import ru.sonarplus.kernel.sqlobject.objects.PredicateInTuple;
import ru.sonarplus.kernel.sqlobject.objects.PredicateIsNull;
import ru.sonarplus.kernel.sqlobject.objects.PredicateLike;
import ru.sonarplus.kernel.sqlobject.objects.PredicateRegExpMatch;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedField;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.QueryParams;
import ru.sonarplus.kernel.sqlobject.objects.Scalar;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumn;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumnsContainer;
import ru.sonarplus.kernel.sqlobject.objects.Source;
import ru.sonarplus.kernel.sqlobject.objects.SourceQuery;
import ru.sonarplus.kernel.sqlobject.objects.SourceTable;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryDelete;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryInsert;
import ru.sonarplus.kernel.sqlobject.objects.SqlQueryUpdate;
import ru.sonarplus.kernel.sqlobject.objects.SqlTransactionCommand;
import ru.sonarplus.kernel.sqlobject.objects.TupleExpressions;
import ru.sonarplus.kernel.sqlobject.objects.UnionItem;
import ru.sonarplus.kernel.sqlobject.objects.UnionsContainer;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;
import ru.sonarplus.kernel.sqlobject.objects.ValueRecId;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;

import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_UNKNOWN;

public class Distiller {

    public static void distillate(SqlObject item, DistillerParams params) {
        DistillationState state = new DistillationState()
                .needResolver(DistillerUtils.containsQRNames(item))
                .setSchema(params.schemaSpec);
        try {
            distillate(item, params, state);
        }
        finally {
            state.clear();
        }
    }

    public static void distillate(SqlObject item, DistillerParams params, DistillationState state) {
        Preconditions.checkNotNull(params);
        Preconditions.checkNotNull(state);

        if (item instanceof  SqlQuery) {
            state.setRoot((SqlQuery)item);
            // ожидаем, что параметры "подняты" из тела запроса в список параметров, а в теле запроса остались только ссылки ParamRef.
            QueryParams queryParams = ((SqlQuery)item).getParams();
            if (queryParams != null)
                for(SqlObject child: queryParams)
                    state.cacheParamsNotUsedYet.add((QueryParam)child);
        }

        SqlObject result;
        try {
            result = internalDistillate(item, params, state);
        }
        finally {
            // покажем накопленные ошибки
            state.throwBulkErrors();
        }

        if (result == null) // паранойя на случай если throwBulkErrors() не сработал.
            throw new DistillationException("");

        if (!state.cacheCTEsNotUsedYet.isEmpty()) {
            // почистим неиспольуемые CTE
            for (CommonTableExpression cte: state.cacheCTEsNotUsedYet)
                cte.getOwner().removeItem(cte);
            state.cacheCTEsNotUsedYet.clear();
        }

        if (!state.cacheParamsNotUsedYet.isEmpty()) {
            // почистим неиспольуемые параметры
            for (QueryParam queryParam: state.cacheParamsNotUsedYet)
                queryParam.getOwner().removeItem(queryParam);
            state.cacheParamsNotUsedYet.clear();
        }

    }

    protected static SqlObject internalDistillate(SqlObject item, DistillerParams params, DistillationState state) {
        Preconditions.checkNotNull(item);

        if (item.getClass() == QualifiedField.class)
            return distillateQField((QualifiedField)item, params, state);

        if (item.getClass() == QualifiedRField.class)
            return distillateQRField((QualifiedRField)item, params, state);

        if (item.getClass() == Expression.class)
            return distillateExpression((Expression)item, params, state);

        if (item.getClass() == CaseSimple.class)
            return distillateCaseSimple((CaseSimple)item, params, state);

        if (item.getClass() == CaseSearch.class)
            return distillateCaseSearch((CaseSearch)item, params, state);

        if (item.getClass() == ParamRef.class)
            return distillateParamRef((ParamRef)item, params, state);

        if (item.getClass() == Scalar.class)
            return distillateScalar((Scalar)item, params, state);

        if (item.getClass() == ValueConst.class)
            return distillateValueConst((ValueConst)item, params, state);


        if (item.getClass() == PredicateComparison.class)
            return distillatePredicateComparison((PredicateComparison)item, params, state);

        if (item.getClass() == PredicateForCodeComparison.class)
            return distillatePredicateForCodeComparison((PredicateForCodeComparison)item, params, state);

        if (item.getClass() == PredicateBetween.class)
            return distillatePredicateBetween((PredicateBetween)item, params, state);

        if (item.getClass() == PredicateExists.class)
            return distillatePredicateExists((PredicateExists)item, params, state);

        if (item.getClass() == PredicateLike.class)
            return distillatePredicateLike((PredicateLike)item, params, state);

        if (item.getClass() == PredicateFullTextSearch.class)
            return distillatePredicateFullTextSearch((PredicateFullTextSearch)item, params, state);

        if (item.getClass() == PredicateRegExpMatch.class)
            return distillatePredicateRegExpMatch((PredicateRegExpMatch)item, params, state);

        if (item.getClass() == PredicateInTuple.class)
            return distillatePredicateInTuple((PredicateInTuple)item, params, state);

        if (item.getClass() == PredicateInQuery.class)
            return distillatePredicateInQuery((PredicateInQuery)item, params, state);

        if (item.getClass() == PredicateIsNull.class)
            return distillatePredicateIsNull((PredicateIsNull)item, params, state);

        if (item.getClass() == Conditions.class)
            return distillateConditions((Conditions)item, params, state);


        if (item.getClass() == CursorSpecification.class)
            return distillateCursorSpecification((CursorSpecification)item, params, state);

        if (item.getClass() == Select.class)
            return distillateSelect((Select)item, params, state);

        if (item.getClass() == SqlQueryInsert.class)
            return distillateInsert((SqlQueryInsert)item, params, state);

        if (item.getClass() == SqlQueryUpdate.class)
            return distillateUpdate((SqlQueryUpdate)item, params, state);

        if (item.getClass() == SqlQueryDelete.class)
            return distillateDelete((SqlQueryDelete)item, params, state);

        if (item.getClass() == CallStoredProcedure.class)
            return distillateCallStoredProcedure((CallStoredProcedure)item, params, state);


        if (item.getClass() == FromContainer.class)
            return distillateFrom((FromContainer)item, params, state);

        if (item.getClass() == CommonTableExpression.class)
            return distillateCTE((CommonTableExpression)item, params, state);

        if (item.getClass() == SelectedColumnsContainer.class)
            return distillateColumns((SelectedColumnsContainer)item, params, state);
        if (item.getClass() == SelectedColumn.class)
            return distillateColumn((SelectedColumn)item, params, state);

        if (item.getClass() == GroupBy.class)
            return distillateGroupBy((GroupBy)item, params, state);

        if (item.getClass() == OrderBy.class)
            return distillateOrderBy((OrderBy)item, params, state);

        if (item.getClass() == OraFTSMarker.class)
            return distillateOraFTSMarker((OraFTSMarker)item, params, state);

        if (item.getClass() == SqlTransactionCommand.class)
            return item; // дистилляция команд по работе с транзакциями не требуется

        state.throwError("Дистилляция объекта не поддержана", item);
        return null;
    }

    protected static boolean canAddOrderByFTSRange(CursorSpecification cursor) {
        OrderBy orderBy = cursor.findOrderBy();
        if (orderBy == null || !orderBy.isHasChilds())
            return true;
        for (SqlObject item: orderBy)
            if (((OrderByItem)item).getExpr().getClass() != OraFTSRange.class)
                return false;
        // можем добавлять сортировки по рангам если нет других сортировок
        // или есть, но только по рангам
        return true;
    }

    protected static SqlObject distillateOraFTSMarker(OraFTSMarker item, DistillerParams params, DistillationState state)
            throws SqlObjectException{
        state.oraFTSMarkersCount++;
        SqlQuery root = state.getRoot();
        SqlObject markerOwner = item.getOwner();
        SqlQuery markerParent = SqlObjectUtils.getParentQuery(item);
        CursorSpecification rootCursor = root.getClass() == CursorSpecification.class ? (CursorSpecification)root : null;
        Select rootCursorSelect = rootCursor != null ? rootCursor.getSelect(): null;
        UnionsContainer rootCursorSelectUnions = rootCursorSelect != null ? rootCursorSelect.findUnions() : null;
        Expression exprMarker = new Expression(String.valueOf(state.oraFTSMarkersCount), true);
        markerOwner.replace(item, exprMarker);
        if (item.sortByScore &&
                rootCursorSelect == markerParent &&
                (rootCursorSelectUnions == null || !rootCursorSelectUnions.isHasChilds()) &&
                canAddOrderByFTSRange(rootCursor)) {
            // попытаемся добавить сортировку по рангам результатов полнотекстового поиска
            // в запрос, содержащий выражение "contains()..."
            rootCursor.newOrderBy()
                    .addOrder(
                            params.dbSupport
                                    // TODO #BAD# "Завязка" на оракловый полнотекстовый поиск
                                    .createFTSRangeExpression(FullTextEngine.ORACLE_TEXT, state.oraFTSMarkersCount),
                            OrderByItem.OrderDirection.DESC,
                            OrderByItem.NullOrdering.NONE);
        }
        return exprMarker;
    }

    protected static SqlObject distillateQField(QualifiedField item, DistillerParams params, DistillationState state)
            throws SqlObjectException {
        if (item.distTechInfo == null) {
            DistillerUtils.checkQFieldUsage(item, null);
            SqlObjectUtils.buildTechInfoForExpr(item, tid_UNKNOWN, true);
        }
        return item;
    }

    protected static SqlObject distillateQRField(QualifiedRField item, DistillerParams params, DistillationState state) {
        DistillerUtils.checkQFieldUsage(item, state.contextParent);
        QualifiedName qname = item.getQName();
        qname.alias = DistillerUtils.calcQFieldAliasPart(item, qname.alias, state.contextParent);

        ColumnExprTechInfo techInfo = state.getResolver().resolve(qname, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
        if (techInfo != null) {
            Preconditions.checkState(!StringUtils.isEmpty(techInfo.nativeFieldName));
            QualifiedField qfield = new QualifiedField(null, qname.alias, techInfo.nativeFieldName);
            qfield.distTechInfo = techInfo;
            return DistillerUtils.replace(item, qfield);
        }

        String orgQName = QualifiedName.formQualifiedNameString(item.getQName().alias, item.getQName().name);
        String sQName = QualifiedName.formQualifiedNameString(qname.alias, qname.name);
        if (!sQName.equals(orgQName))
            sQName = sQName + '(' + orgQName + ')';
        state.throwError(String.format("Не удалось сопоставить имя '%s' с полями таблиц схемы и табличных выражений запроса", sQName), item);
        return null;
    }

    protected static void distillateFieldBinToText(String tableAlias, ColumnExprTechInfo fieldSpec,
                                            Expression expr,
                                                   DistillerParams params, DistillationState state) {
        Expression binToTextExpr = params.dbSupport.getExpressionForBinaryField(tableAlias, fieldSpec);
        expr.setExpr(binToTextExpr.getExpr());
        expr.insertItem(binToTextExpr.firstSubItem());
        internalDistillate(expr.firstSubItem(), params, state);
    }

    protected static boolean tryReplaceFieldsBinToText(Expression expr,
                                                DistillerParams params, DistillationState state) {
        String[] binToTextFields = ExprUtils.exprExtractBinToTextFields(expr.getExpr());
        if (binToTextFields.length == 0)
            return true;

        // после применения к выражению процедуры построения дерева выражений,
        // выражение FieldBinToText содержится целиком в объекте TExpression,
        // не содержащем более ничего, в т.ч. подчинённых компонентов-аргументов.
        // Поэтому количество извлечённых имён полей = 1
        Preconditions.checkState(binToTextFields.length == 1);
        Preconditions.checkState(!expr.isHasChilds());
        String sField = binToTextFields[0];
        // TODO...нужна ли эта функция??
        QualifiedName qName = QualifiedName.stringToQualifiedName(sField);
        qName.alias = DistillerUtils.calcQFieldAliasPart(expr, qName.alias, state.contextParent);

        ColumnExprTechInfo fieldSpec = state.getResolver().resolve(qName, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
        if (fieldSpec != null) {
            distillateFieldBinToText(qName.alias, fieldSpec, expr, params, state);
            return true;
        }
        else {
            state.throwError(
                    String.format(
                            "Не удалось сопоставить имя '%s' с полями таблиц схемы и табличных выражений запроса",
                            qName.qualifiedNameToString()),
                    expr
            );
            return false;
        }
    }

    protected static SqlObject internalDistillateExpression(Expression item, DistillerParams params, DistillationState state) {
        FieldTypeId back = state.getNeededType();
        try {
            state.setNeededType(tid_UNKNOWN); // чтобы не пытаться приводить аргументы-константы к типу поля отключим требование типа
            boolean distillated = true;
            if (SqlObjectUtils.isPureAsterisk(item)) {
                DistillerUtils.checkPureAsteriskPossibleHere(item);
                SqlObjectUtils.buildTechInfoForExpr(item, tid_UNKNOWN);
                return item;
            }
            //выполнить дистиляцию объектов, подчиненных выражению - его аргументы
            for (int i = 0; i < item.itemsCount(); i++)
                //TODO 'SUM(BYTE)'. Может встретиться запрос, в котором будет выполнятьcя суммирование по байтовому полю RAW(0).
                // В Delphi, при трансляции интерактивных запросов в трансляторе запроса "Сравнение полей"
                // для таких полей при необходимости создаётся обёртка (TO_NUMBER) с помощью функции DBSupport.BuildByteValueExpr.
                // при дистилляции для таких выражений сейчас ничего не делается. В результате, при выполнении запроса,
                // можем получить ошибку Oracle: ORA-00932: несовместимые типы данных: ожидается NUMBER, получено BINARY.
                // нужно подумать, как обойти ситуацию.
                // Гипотетически - для аргумента выражения типа BYTE, используемого в функциях SUM/AVG - здесь вызывать DBSupport.BuildByteValueExpr.
                // Вопрос в том, как определить, что для байтового выражения действительно требуется создать обёртку,
                // т.к. поддержка работы с выражениями весьма простая...
                distillated = distillated && internalDistillate(item.getItem(i), params, state) != null;

            if (!item.isPureSql) {
                //избавиться от разметки в строке выражения
                distillated = distillated && tryReplaceFieldsBinToText(item, params, state);
                FunctionsReplacer.execute(SqlObjectUtils.getRootQuery(item), item, params.dbSupport, params.fixedCurrentDateTime);
            }

            ColumnExpression result = null;
            if (item.getExpr().equals(Expression.UNNAMED_ARGUMENT_REF)) { // если выражение представляет собой только ссылку на аргумент...
                result = (ColumnExpression) item.firstSubItem();
                result.distTechInfo = item.distTechInfo;
                DistillerUtils.replace(item, result); // ...заменим выражение его аргументом
            } else {
                if (DistillerUtils.getUnnamedRefCount(item.getExpr(), state.cacheExprRefCounts) != item.itemsCount())
                    state.throwError(
                            String.format(
                                    ExprUtils.NOT_CORRESPONDING_UNNAMED_ARG_REFS_WITH_EXPR_CHILDS,
                                    item.getExpr()),
                            item
                    );
                result = item;
            }

            if (distillated && result instanceof Expression) {
                // ничего не знаю о результате выражения и его регистрозависимости
                SqlObjectUtils.buildTechInfoForExpr(result, tid_UNKNOWN);
                // выставим флаг "чистый sql"
                ((Expression) result).isPureSql = true;
            }

            return distillated ? result : null;
        }
        finally {
            state.setNeededType(back);
        }
    }

    protected static SqlObject distillateExpression(Expression item, DistillerParams params, DistillationState state) {
        boolean isExpressionTreeBuiltHere = false;
        ColumnExpression newExpr  = null;
        if (item.getExpr().contains(ExprConsts.EXPR_BEGIN) && !state.isExpressionTreeBuilt()) {
            // если это строка с "нашим" выражением - преобразуем её в дерево, если этого не произошло ранее
            newExpr = (ColumnExpression) SqlObjectUtils.buildTreesForExpressions(item);
            // отметим этот факт, чтобы при дистилляции аргументов опять не строить дерево
            state.setExpressionTreeBuilt(true);
            isExpressionTreeBuiltHere = true;
        }
        else
            newExpr = item;

        try {
            if (newExpr instanceof Expression)
                return internalDistillateExpression((Expression) newExpr, params, state);
            else
                // строка выражения могла "свернуться", например, в QField, Case и т.д.
                return internalDistillate(newExpr, params, state);
        }
        finally{
            if (isExpressionTreeBuiltHere)
                // с этим выражением закончили - флаг сбросим
                state.setExpressionTreeBuilt(false);
        }
    }

    protected static SqlObject distillateCaseSimple(CaseSimple item, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        List<ColumnExprTechInfo> distInfosCaseWhen = new ArrayList<>();
        List<ColumnExprTechInfo> distInfosThenElse = new ArrayList<>();
        List<ColumnExpression> caseWhenParamsAndValues = new ArrayList<>();
        List<ColumnExpression> thenElseParamsAndValues = new ArrayList<>();
        ColumnExpression expr;

        // сначала дистиллируем поля и выражения исключая значения и параметры
        expr = item.getCase();
        if (expr instanceof ValueConst || expr instanceof ParamRef)
            // значения и параметры будем дистиллировать с учётом информации о полях
            caseWhenParamsAndValues.add(expr);
        else {
            distillated = distillated && (internalDistillate(item.getCase(), params, state) != null);
            distInfosCaseWhen.add(item.getCase().distTechInfo);
        }

        for (SqlObject child: item)
            if (child instanceof CaseSimple.WhenThen) {
                CaseSimple.WhenThen whenThen = (CaseSimple.WhenThen) child;
                expr = whenThen.getWhen();
                if (expr instanceof ValueConst || expr instanceof ParamRef)
                    caseWhenParamsAndValues.add(expr);
                else {
                    distillated = distillated && (internalDistillate(expr, params, state) != null);
                    distInfosCaseWhen.add(whenThen.getWhen().distTechInfo);
                }

                expr = whenThen.getThen();
                if (expr instanceof ValueConst || expr instanceof ParamRef)
                    thenElseParamsAndValues.add(expr);
                else {
                    distillated = distillated && (internalDistillate(expr, params, state) != null);
                    distInfosThenElse.add(whenThen.getThen().distTechInfo);
                }
            }
        expr = item.getElse();
        if (expr != null) {
            if (expr instanceof ValueConst || expr instanceof ParamRef)
                thenElseParamsAndValues.add(expr);
            else {
                distillated = distillated && (internalDistillate(expr, params, state) != null);
                distInfosThenElse.add(item.getElse().distTechInfo);
            }
        }

        // определим регистронезависомость и тип для выражений CASE-WHEN
        ColumnExprTechInfo distInfoCaseWhen = new ColumnExprTechInfo();
        for (ColumnExprTechInfo techInfo: distInfosCaseWhen)
            if (techInfo != null) {
                distInfoCaseWhen.caseSensitive = distInfoCaseWhen.caseSensitive && techInfo.caseSensitive;
                if (techInfo.fieldTypeId != tid_UNKNOWN) {
                    if (distInfoCaseWhen.fieldTypeId == tid_UNKNOWN)
                        distInfoCaseWhen.fieldTypeId = techInfo.fieldTypeId;
                    else if (techInfo.fieldTypeId != distInfoCaseWhen.fieldTypeId) {
                        state.throwError(
                                String.format("В конструкции CASE несовместимые типы выражений %s/%s", techInfo.fieldTypeId, distInfoCaseWhen.fieldTypeId),
                                item
                        );
                        distillated = false;
                    }
                }
            }

        FieldTypeId neededTypeIdBack;

        // выполним дистилляцию значений и параметров в CASE/WHEN с учётом distInfoCaseWhen
        neededTypeIdBack = state.getNeededType();
        try {
            state.setNeededType(distInfoCaseWhen.fieldTypeId);
            for (ColumnExpression exprValue : caseWhenParamsAndValues)
                distillated = distillated && (internalDistillate(exprValue, params, state) != null);
        }
        finally {
            state.setNeededType(neededTypeIdBack);
        }

        // определим регистронезависимость и тип для выражений THEN/ELSE
        ColumnExprTechInfo distInfoThenElse = new ColumnExprTechInfo(); // эта информация будет после дистилляции сопоставлена выражению CASE
        for (ColumnExprTechInfo techInfo: distInfosThenElse)
            if (techInfo != null) {
                distInfoThenElse.caseSensitive = distInfoThenElse.caseSensitive && techInfo.caseSensitive;
                if (techInfo.fieldTypeId != tid_UNKNOWN) {
                    if (distInfoThenElse.fieldTypeId == tid_UNKNOWN)
                        distInfoThenElse.fieldTypeId = techInfo.fieldTypeId;
                    else if (techInfo.fieldTypeId != distInfoThenElse.fieldTypeId)
                        state.throwError(
                                String.format("В конструкции CASE несовместимые типы выражений %s/%s", techInfo.fieldTypeId, distInfoCaseWhen.fieldTypeId),
                                item
                        );
                }
            }
        // выражение case может участвовать в к-л сравнении, где может требоваться определённый тип выражения
        if (state.getNeededType() != tid_UNKNOWN &&
                distInfoThenElse.fieldTypeId != tid_UNKNOWN &&
                state.getNeededType() != distInfoThenElse.fieldTypeId) {
            state.throwError(
                    String.format("Тип выражения CASE '%' не соответствует типу '%', требуемому в условии", distInfoThenElse.fieldTypeId, state.getNeededType()),
                    item
            );
            distillated = false;
        }

        // выполним дистилляцию значений и параметров в THEN/ELSE с учётом distInfoThenElse
        neededTypeIdBack = state.getNeededType();
        try {
            state.setNeededType(distInfoThenElse.fieldTypeId);
            for (ColumnExpression exprValue : thenElseParamsAndValues)
                distillated = distillated && (internalDistillate(exprValue, params, state) != null);
        }
        finally {
            state.setNeededType(neededTypeIdBack);
        }

        if (distillated) {
            item.distTechInfo = distInfoThenElse;
            if (!distInfoCaseWhen.caseSensitive) {
                for (SqlObject child: item)
                    if (child instanceof CaseSimple.WhenThen) {
                        CaseSimple.WhenThen whenThen = (CaseSimple.WhenThen) child;
                        DistillerUtils.applyUpper(whenThen.getWhen(), params);
                    }
                DistillerUtils.applyUpper(item.getCase(), params);
            }

            if (!distInfoThenElse.caseSensitive)
                return DistillerUtils.applyUpper(item, params);
            else
                return item;
        }
        else
            return null;
    }

    protected static SqlObject distillateCaseSearch(CaseSearch item, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        List<ColumnExprTechInfo> distInfosThenElse = new ArrayList<>();
        List<ColumnExpression> thenElseParamsAndValues = new ArrayList<>();
        ColumnExpression expr;

        for (SqlObject child: item)
            if (child instanceof CaseSearch.WhenThen) {
                CaseSearch.WhenThen whenThen = (CaseSearch.WhenThen) child;
                distillated = distillated && (internalDistillate(whenThen.getWhen(), params, state) != null);

                expr = whenThen.getThen();
                if (expr instanceof ValueConst || expr instanceof ParamRef)
                    thenElseParamsAndValues.add(expr);
                else {
                    distillated = distillated && (internalDistillate(expr, params, state) != null);
                    distInfosThenElse.add(whenThen.getThen().distTechInfo);
                }
            }

        expr = item.getElse();
        if (expr != null) {
            if (expr instanceof ValueConst || expr instanceof ParamRef)
                thenElseParamsAndValues.add(expr);
            else {
                distillated = distillated && (internalDistillate(expr, params, state) != null);
                distInfosThenElse.add(item.getElse().distTechInfo);
            }
        }

        // определим регистронезависимость и тип для выражений THEN/ELSE
        ColumnExprTechInfo distInfoThenElse = new ColumnExprTechInfo(); // эта информация будет после дистилляции сопоставлена выражению CASE
        for (ColumnExprTechInfo techInfo: distInfosThenElse)
            if (techInfo != null) {
                distInfoThenElse.caseSensitive = distInfoThenElse.caseSensitive && techInfo.caseSensitive;
                if (techInfo.fieldTypeId != tid_UNKNOWN) {
                    if (distInfoThenElse.fieldTypeId == tid_UNKNOWN)
                        distInfoThenElse.fieldTypeId = techInfo.fieldTypeId;
                    else if (techInfo.fieldTypeId != distInfoThenElse.fieldTypeId) {
                        state.throwError(
                                String.format("В конструкции CASE несовместимые типы выражений %s/%s", techInfo.fieldTypeId, distInfoThenElse.fieldTypeId),
                                item
                        );
                        distillated = false;
                    }
                }
            }
        // выражение case может участвовать в к-л сравнении, где может требоваться определённый тип выражения
        if (state.getNeededType() != tid_UNKNOWN &&
                distInfoThenElse.fieldTypeId != tid_UNKNOWN &&
                state.getNeededType() != distInfoThenElse.fieldTypeId) {
            state.throwError(
                    String.format("Тип выражения CASE '%' не соответствует типу '%', требуемому в сравнении", distInfoThenElse.fieldTypeId, state.getNeededType()),
                    item
            );
            distillated = false;
        }

        // выполним дистилляцию значений и параметров в THEN/ELSE с учётом distInfoThenElse
        FieldTypeId neededTypeIdBack = state.getNeededType();
        try {
            state.setNeededType(distInfoThenElse.fieldTypeId);
            for (ColumnExpression exprValue : thenElseParamsAndValues)
                distillated = distillated && (internalDistillate(exprValue, params, state) != null);
        }
        finally {
            state.setNeededType(neededTypeIdBack);
        }

        if (distillated) {
            item.distTechInfo = distInfoThenElse;
            if (!distInfoThenElse.caseSensitive)
                return DistillerUtils.applyUpper(item, params);
            else
                return item;
        }
        else
            return null;
    }

    protected static SqlObject distillateParamRefWithoutQueryParam(ParamRef item, DistillerParams params, DistillationState state) {
        if (state.getNeededType() == tid_UNKNOWN) {
            state.throwError(
                    String.format(
                            "Для ссылки на параметр '%s' не удаётся определить тип значения параметра в связи с: " +
                                    "\n- отсутствием параметра;" +
                                    "\n- отсутствием информации о поле, сопоставляемом с параметром",
                            item.parameterName
                    ),
                    item);
            return null;
        }
        FieldTypeId fixedValueType = state.cacheParamsFixedValueTypes.get(item.parameterName);
        if (fixedValueType == null) {
            fixedValueType = state.getNeededType();
            state.cacheParamsFixedValueTypes.put(item.parameterName, fixedValueType);
        }
        else if (fixedValueType != state.getNeededType()) {
            state.throwError(
                    String.format(
                            "Для ссылки на параметр '%s' ранее уже был использован тип '%s' отличный от '%s'.",
                            item.parameterName, fixedValueType.toString(), state.getNeededType().toString()
                            ),
                    item);
            return null;
        }
        SqlObject owner = item.getOwner();
        ColumnExpression wrapper = params.dbSupport.wrapParamRefToExprInDistillationContext(item, fixedValueType);
        if (wrapper == item)
            return item;

        wrapper.distTechInfo = item.distTechInfo;
        owner.replace(item, wrapper);
        return wrapper;
    }

    protected static SqlObject distillateParamRefWithQueryParam(ParamRef item, QueryParam queryParam, DistillerParams params, DistillationState state) {
        if (item.getOwner() == null ||
                /*
                    Параметр кодификаторного условия при трансляции "породит" ряд параметров для корня, границ диапазона, и т.п.
                    При этом сам исходный параметр использоваться не будет, в связи с чем его не отмечаем, как используемый,
                    что приведёт к его удалению после дистилляции, чтобы в перечне параметров не висели 'бесхозные' параметры.
                */
                item.getOwner().getClass() != PredicateForCodeComparison.class)
            state.cacheParamsNotUsedYet.remove(queryParam);
        FieldTypeId neededValueType = state.getNeededType();
        if (neededValueType == tid_UNKNOWN)
            // неизвестно, с чем сопоставляем. используем тип значения параметра
            neededValueType = queryParam.getValueType();
        else if (queryParam.getValueType() != neededValueType) { // в контексте использования параметра известен его требуемый тип
            // тип значения параметра не совпадает с требуемым в данном контексте...
            // посмотрим - ранее этот параметр где-то уже использовался?
            FieldTypeId fixedValueType = state.cacheParamsFixedValueTypes.get(item.parameterName);
            if (fixedValueType == null) {
                // ещё нет. попытаемся привести параметр к требуемому типу
                queryParam.setValueType(neededValueType);
                // зафиксируем эту информацию
                state.cacheParamsFixedValueTypes.put(item.parameterName, neededValueType);
            } else if (fixedValueType == neededValueType) {
                // всё отлично, никакого противоречия в использовании одного и того же параметра в разных ситуациях
            } else {
                // параметру уже задали ранее иной тип значения
                state.throwError(
                        String.format(
                                "Для параметра '%s' уже был задан тип значения '%s', отличный от задаваемого '%s'",
                                item.parameterName, fixedValueType, neededValueType),
                        item
                );
                return null;
            }
        }

        // создадим нужные выражения - обёртки
        SqlObjectUtils.buildTechInfoForExpr(item, neededValueType, true);
        SqlObject owner = item.getOwner();
        ColumnExpression wrapper;
        if (owner.getClass() == PredicateForCodeComparison.class)
            // для параметра в контектсе кодификаторного условия обёрток никаких не создаём.
            // всё необходимое будет выполнено при трансляции кодификаторного условия
            wrapper = item;
        else
            wrapper = params.dbSupport.wrapParamRefToExprInDistillationContext(item, neededValueType);
        if (wrapper == item)
            return item;

        wrapper.distTechInfo = item.distTechInfo;
        owner.replace(item, wrapper);
        return wrapper;
    }

    protected static SqlObject distillateParamRef(ParamRef item, DistillerParams params, DistillationState state) {
        QueryParam qparam = null;
        QueryParams queryParams = state.getRoot().getParams();
        if (queryParams != null)
            qparam = queryParams.findParam(item.parameterName);
        if (qparam == null)
            return distillateParamRefWithoutQueryParam(item, params, state);

        DistillerUtils.checkParamRefUsage(item, qparam);
        return distillateParamRefWithQueryParam(item, qparam, params, state);
    }

    protected static SqlObject distillateScalar(Scalar item, DistillerParams params, DistillationState state) {
        internalDistillate(item.findSelect(), params, state);
        SelectedColumnsContainer columns = item.findSelect().getColumns();
        int columnsCount = columns == null ? 0 : columns.itemsCount();
        if (columnsCount != 1) {
            state.throwError(
                    String.format(
                            "В скалярном запросе должна быть ровно одна колонка, присутствует  %d",
                            columnsCount
                    ),
                    item
            );
            return null;
        }
        ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(columns.getColumn(0).getColExpr()).getClone();
        techInfo.nativeFieldName = "";
        techInfo.dbdFieldName = "";
        item.distTechInfo = techInfo;
        return item;
    }

    protected static SqlObject distillateValueConst(ValueConst item, DistillerParams params, DistillationState state)
            throws SqlObjectException{
        if ((state.getNeededType() != tid_UNKNOWN) && (item.getValueType() != state.getNeededType()))
            // приведём значение к нужному типу
            item.setValueType(state.getNeededType());
        ColumnExpression result = params.dbSupport.createColumnExprForValue(item.getValue(), item.getValueType());
        SqlObjectUtils.buildTechInfoForExpr(result, item.getValueType(), true);
        return DistillerUtils.replace(item, result);
    }

    protected static boolean tryDistillateComparisonOperands(ColumnExpression op1, ColumnExpression op2, DistillerParams params, DistillationState state) {
        ColumnExpression distillatedOp1 = (ColumnExpression)internalDistillate(op1, params, state);
        boolean distillated = distillatedOp1 != null;
        if (distillated) {
            FieldTypeId back = state.getNeededType();
            try {
                state.setNeededType(Utils.getFieldTypeId(distillatedOp1));
                distillated = internalDistillate(op2, params, state) != null;
            } finally {
                state.setNeededType(back);
            }
        }
        return distillated;
    }

    protected static int getComparisonKind(PredicateComparison item, DistillerParams params, DistillationState state)
            throws SqlObjectException{
        // 0 - обычное сравнение, 1-по recid, -1 - ошибка
        ColumnExpression left = item.getLeft();
        ColumnExpression right = item.getRight();
        return SqlObjectUtils.isRecordId(left) ||
                SqlObjectUtils.isRecordId(right) ||
                DistillerUtils.isParamValueRecId(left, state) ||
                DistillerUtils.isParamValueRecId(right, state) ? 1 : 0;
    }

    protected static ColumnExpression getRecIdCmpLeft(PredicateComparison item) {
        if (SqlObjectUtils.isRecordId(item.getLeft()))
            return item.getLeft();
        else
            return item.getRight();
    }

    protected static ColumnExpression getRecIdCmpRight(PredicateComparison item) {
        if (SqlObjectUtils.isRecordId(item.getLeft()))
            return item.getRight();
        else
            return item.getLeft();
    }

    protected static SqlObject distillateRecordIdComparisonRecIdAndParam(PredicateComparison item, DistillerParams params, DistillationState state) {
        QualifiedField recordId = (QualifiedField)getRecIdCmpLeft(item);
        QueryParam paramValueRecId = state.getRoot().getParams().findExistingParam(((ParamRef)getRecIdCmpRight(item)).parameterName);
        ValueRecId valueRecId = (ValueRecId) paramValueRecId.getValueObj();
        QualifiedName qname = recordId.getQName();
        qname.alias = DistillerUtils.calcQFieldAliasPart(item, qname.alias, state.contextParent);
        ColumnExprTechInfo[] techInfo = state.getResolver().getRecId(qname.alias, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
        if (techInfo == null || techInfo.length == 0) {
            state.throwError("Не удалось развернуть RecordId() для таблицы " + qname.alias, item);
            return null;
        }

        if (valueRecId.getRecId().count() != 0 && techInfo.length != valueRecId.getRecId().count()) {
            state.throwError("Количество значений recid не совпадает с количеством полей RecordId()", item);
            return null;
        }

        if (techInfo.length > 1) {
            state.throwError(String.format("Сравнение по составным RecId не поддержано: %s", qname.qualifiedNameToString()), item);
            return null;
        }

        item.setLeft(new QualifiedField(null, qname.alias, techInfo[0].nativeFieldName));
        item.getLeft().distTechInfo = techInfo[0];
        // TODO сравнение с null -recid. Нужно проверить как выполняются отчёты и т.д
        if (valueRecId.getRecId().count() != 0) {
            Object value = valueRecId.getRecId().getValue(0);
            if (value == null) {
                if (params.useStandartNulls)
                    paramValueRecId.setValueObj(ValueConst.createNull());
                else // TODO а нужна ли эта замена null-значений параметра recid соответствующими нулевыми?
                    paramValueRecId.setValueObj(
                            new ValueConst(
                                    ValuesSupport.getZeroValue(techInfo[0].fieldTypeId),
                                    techInfo[0].fieldTypeId
                            ));
            }
            else {
                paramValueRecId.setValueObj(
                        new ValueConst(
                                value,
                                techInfo[0].fieldTypeId
                        ));
            }
        }
        else {
            if (params.useStandartNulls)
                paramValueRecId.setValueObj(ValueConst.createNull());
            else
                paramValueRecId.setValueObj(
                        new ValueConst(
                                ValuesSupport.getZeroValue(techInfo[0].fieldTypeId),
                                techInfo[0].fieldTypeId
                        ));
        }
        // из списка неиспользованных его, конечно, удалили
        // но не проверяем, использовался ли этот параметр где-то с другим полем. но может быть этого и не нужно...
        state.cacheParamsNotUsedYet.remove(paramValueRecId);
        return item;
    }

    protected static SqlObject distillateRecordIdComparisonBothRecIds(
            PredicateComparison item, DistillationState state) {
        QualifiedName qNameLeft = ((QualifiedField)item.getLeft()).getQName();
        QualifiedName qNameRight = ((QualifiedField)item.getRight()).getQName();

        qNameLeft.alias = DistillerUtils.calcQFieldAliasPart(item, qNameLeft.alias, state.contextParent);
        ColumnExprTechInfo[] techInfoLeft = state.getResolver().getRecId(qNameLeft.alias, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
        if (techInfoLeft == null || techInfoLeft.length == 0) {
            state.throwError("Не удалось развернуть RecordId() для таблицы " + qNameLeft.alias, item);
            return null;
        }

        qNameRight.alias = DistillerUtils.calcQFieldAliasPart(item, qNameRight.alias, state.contextParent);
        ColumnExprTechInfo[] techInfoRight = state.getResolver().getRecId(qNameRight.alias, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
        if (techInfoRight == null || techInfoRight.length == 0) {
            state.throwError("Не удалось развернуть RecordId() для таблицы " + qNameRight.alias, item);
            return null;
        }

        if (techInfoLeft.length != techInfoRight.length) {
            state.throwError(String.format(
                    "В сравнении по RecId несовместимые по составу полей аргументы: %s, %s",
                    qNameLeft.qualifiedNameToString(), qNameRight.qualifiedNameToString()),
                    item);
            return null;
        }

        if (techInfoLeft.length > 1) {
            state.throwError(String.format("Сравнение по составным RecId не поддержано: %s", qNameLeft.qualifiedNameToString()), item);
            return null;
        }
        if (techInfoRight.length > 1) {
            state.throwError(String.format("Сравнение по составным RecId не поддержано: %s", qNameRight.qualifiedNameToString()), item);
            return null;
        }

        if (techInfoLeft[0].fieldTypeId != techInfoRight[0].fieldTypeId) {
            state.throwError(
                    String.format(
                            "В сравнении по RecId разные по типам значений операнды: %s:%s - %s:%s",
                            qNameLeft.qualifiedNameToString(),
                            techInfoLeft[0].fieldTypeId,
                            qNameRight.qualifiedNameToString(),
                            techInfoRight[0].fieldTypeId
                            ), item);
            return null;
        }

        item.setLeft(new QualifiedField(null, qNameLeft.alias, techInfoLeft[0].nativeFieldName));
        item.setRight(new QualifiedField(null, qNameRight.alias, techInfoRight[0].nativeFieldName));
        return item;
    }

    protected static boolean validRecIdCmpOperands(PredicateComparison item, DistillationState state)
            throws DistillationException {
        final String invalidCmp = "В условии по RecId допускается сравнение идентификатора с другим идентификатором или с параметром, содержащим значение RecId";
        boolean res =
                (
                        SqlObjectUtils.isRecordId(item.getLeft()) && (
                                SqlObjectUtils.isRecordId(item.getRight()) ||
                                DistillerUtils.isParamValueRecId(item.getRight(), state)
                        )
                ) ||
                (DistillerUtils.isParamValueRecId(item.getLeft(), state) && SqlObjectUtils.isRecordId(item.getRight()));

        if (!res)
            // один из операндов не RecordId или Param(ValueRecId)
            // или оба - Param(ValueRecId)
            // хотя, наверное, можно было бы предусмотреть и CASE и SCALAR
            state.throwError(invalidCmp, item.getLeft());

        return res;
    }

    protected static SqlObject distillateRecordIdComparison(PredicateComparison item, DistillerParams params, DistillationState state) {
        if (item.comparison != PredicateComparison.ComparisonOperation.EQUAL && item.comparison != PredicateComparison.ComparisonOperation.NOT_EQUAL) {
            state.throwError("Сравнение по recid может быть только на (не)равенство", item);
            return null;
        }
        if (!validRecIdCmpOperands(item, state))
            return null;

        if (SqlObjectUtils.isRecordId(item.getRight()))
            return distillateRecordIdComparisonBothRecIds(item, state);
        else
            return distillateRecordIdComparisonRecIdAndParam(item, params, state);
    }

    protected static SqlObject distillatePredicateComparison(PredicateComparison item, DistillerParams params, DistillationState state) {

        switch (getComparisonKind(item, params, state)) {
            case 0: // обычное
                break;
            case 1: // по recid
                // в условии с recid заменяем операнды настоящим (уже дистиллированным) полем и ссылкой на параметр с одиночным значением
                if (distillateRecordIdComparison(item, params, state) != null)
                  // а затем, чтобы создались нужные обёртки - дистиллируем как обычно
                  break;
            case -1: // ошибочное
                return null;
            default:
                Preconditions.checkState(false);
        }

        ColumnExpression right = item.getRight();
        ColumnExpression left = item.getLeft();

        boolean distillated;
        if (left instanceof ValueConst || left instanceof ParamRef)
            distillated = tryDistillateComparisonOperands(right, left, params, state);
        else
            distillated = tryDistillateComparisonOperands(left, right, params, state);

        if (distillated) {
            // после дистилляции в качестве операндов будут уже другие объекты
            right = item.getRight();
            left = item.getLeft();
            boolean caseSensitive = left.distTechInfo.caseSensitive && right.distTechInfo.caseSensitive;
            left.distTechInfo.caseSensitive = caseSensitive;
            right.distTechInfo.caseSensitive = caseSensitive;
            DistillerUtils.applyUpperIfNotCaseSensitive(left, params);
            DistillerUtils.applyUpperIfNotCaseSensitive(right, params);
        }

        return distillated ? item: null;
    }

    protected static SqlObject distillatePredicateForCodeComparison(PredicateForCodeComparison item, DistillerParams params, DistillationState state) {
        Predicate result = null;
        QualifiedField left = item.getField();
        if (left == null) {
            state.throwError("Не задано поле в кодификаторном условии.", item);
            return null;
        }

        if (left.getClass() != QualifiedRField.class) {
            // #BAD# Завязались на то, что в кодификаторном условии может быть только QualifiedRField.
            // Связано с тем, что для него можем получить из описателя всю необходимую информацию, в т.ч. размер кода кодификатора,
            // который, в свою очередь, понадобится для вычисления диапазона, уровня...
            // Если в условии будет QualifiedField - его distTechInfo будет заполнен минимально и не будет содержать информацию о размере кода
            state.throwError(String.format("В кодификаторном условии поле условия доложно быть %s", QualifiedRField.class.getSimpleName()), item);
            return null;
        }

        boolean distillated = internalDistillate(left, params, state) != null;
        ColumnExpression code = item.getCode();
        if (code == null) {
            state.throwError(
                    "Не задано значение для кодификаторного условия",
                    item
            );
            distillated = false;
        }

        if (distillated) {
            ColumnExprTechInfo fieldTechInfo = SqlObjectUtils.getTechInfo(item.getField());
            if (!(fieldTechInfo.fieldTypeId == FieldTypeId.tid_CODE || fieldTechInfo.fieldTypeId == tid_UNKNOWN)) {
                distillated = false;
                state.throwError(
                        String.format("Поле '%s' в условии %s должно иметь тип %s, получен %s",
                                item.getField().getQName().qualifiedNameToString(),
                                item.getClass().getName(),
                                FieldTypeId.tid_CODE.toString(),
                                fieldTechInfo.fieldTypeId.toString()),
                        item
                );
            }
            else if (code instanceof ValueConst) {
                // TODO Код кодификатора сделаем нужного размера?
            }
            else if (code instanceof ParamRef) {
                FieldTypeId backNeedType = state.getNeededType();
                try {
                    state.setNeededType(FieldTypeId.tid_CODE);
                    distillateParamRef((ParamRef)code, params, state);
                }
                finally {
                    state.setNeededType(backNeedType);
                }
            }
            else {
                state.throwError(
                        String.format(
                                "Не реализована дистилляция для значения кода '%' в кодификаторном условии",
                                code.getClass().getName()),
                        item
                );
                distillated = false;
            }
            if (distillated) {
                // создадим описатель для значения кода кодификатора
                SqlObjectUtils.buildTechInfoForExpr(code, tid_UNKNOWN, true);
                // обязательно выставим размер кода кодификатора. понадобится при вычислении диапазонов и проч..
                code.distTechInfo.bytesForCode = item.getField().distTechInfo.bytesForCode;
                // развернём в набор простых условий
                result = PredicateForCodeComparisonTranslator.translate(item, params.dbSupport);
                DistillerUtils.replace(item, result);
            }
        }
        return result;
    }

    protected static SqlObject distillatePredicateBetween(PredicateBetween item, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        ColumnExpression operands[] = new ColumnExpression[3];
        ColumnExpression values[] = new ColumnExpression[3];
        int i = 0;
        for (SqlObject child: item)
            if (child instanceof ColumnExpression)
                if (child instanceof ValueConst || child instanceof ParamRef)
                    values[i++] = (ColumnExpression) child;
                else
                    operands[i++] = (ColumnExpression) child;

        ColumnExprTechInfo distInfo = new ColumnExprTechInfo();
        for (ColumnExpression expr: operands) {
            if (expr == null)
                continue;
            ColumnExpression dist = (ColumnExpression) internalDistillate(expr, params, state);
            distillated = distillated && dist != null;
            if (distillated) {
                ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(dist);
                distInfo.caseSensitive = distInfo.caseSensitive && techInfo.caseSensitive;
                if (distInfo.fieldTypeId == tid_UNKNOWN)
                    distInfo.fieldTypeId = techInfo.fieldTypeId;
                else if (techInfo.fieldTypeId != tid_UNKNOWN && techInfo.fieldTypeId != distInfo.fieldTypeId){
                    state.throwError(
                            String.format(
                                    "В условии BETWEEN несовместимые  операнды %s-%s",
                                    distInfo.fieldTypeId,
                                    techInfo.fieldTypeId),
                            item
                    );
                    distillated = false;
                }
            }
        }
        if (distillated) {
            FieldTypeId back = state.getNeededType();
            try {
                state.setNeededType(distInfo.fieldTypeId);
                for (ColumnExpression value : values)
                    if (value != null)
                        internalDistillate(value, params, state);
            } finally {
                state.setNeededType(back);
            }

            if (!distInfo.caseSensitive) {
                DistillerUtils.applyUpper(item.getExpr(), params);
                DistillerUtils.applyUpper(item.getLeft(), params);
                DistillerUtils.applyUpper(item.getRight(), params);
            }
        }
        return distillated ? item : null;
    }

    protected static SqlObject distillatePredicateExists(PredicateExists item, DistillerParams params, DistillationState state) {
        Select select = item.findSelect();
        if (select == null) {
            state.throwError(
                    String.format(
                            "В условии '%s' не задан подзапрос",
                            item.getClass().getName()),
                    item
            );
            return null;
        }
        // TODO - можно подзапросу в Exists удалить все его колонки, создав взамен одну, например '1'. нужно ли это?
        return internalDistillate(item.findSelect(), params, state) != null ? item : null;
    }

    protected static SqlObject distillatePredicateLike(PredicateLike item, DistillerParams params, DistillationState state) {
        ColumnExpression expr = (ColumnExpression)internalDistillate(item.getLeft(), params, state);
        ColumnExpression template = (ColumnExpression)internalDistillate(item.getRight(), params, state);
        if (expr != null && template != null) {
            Preconditions.checkState(expr.distTechInfo != null && template.distTechInfo != null);
            boolean caseSensitive = expr.distTechInfo.caseSensitive && template.distTechInfo.caseSensitive;
            expr.distTechInfo.caseSensitive = caseSensitive;
            template.distTechInfo.caseSensitive = caseSensitive;
            DistillerUtils.applyUpperIfNotCaseSensitive(expr, params);
            DistillerUtils.applyUpperIfNotCaseSensitive(template, params);
            return item;
        }
        return null;
    }

    protected static SqlObject distillatePredicateFullTextSearch(PredicateFullTextSearch item, DistillerParams params, DistillationState state) {
        ColumnExpression left = item.getLeft();
        if (left instanceof QualifiedRField) {
            QualifiedRField qfield = (QualifiedRField) left;
            qfield.alias = DistillerUtils.calcQFieldAliasPart(qfield, state.contextParent);
        }
        Predicate result = params.dbSupport.createFTSExpression(FullTextEngine.ORACLE_TEXT, item);
        if (result == null)
            state.throwError(
                    String.format(
                            "Не удалось дистиллировать условие '%s'",
                            item.getClass().getName()
                    ),
                    item
            );
        else if (result instanceof PredicateComparison || result instanceof PredicateLike || result instanceof Conditions) {
            // все содержащиеся в условии строковые выражения, построенные с помощью функций EXPR_...
            // сконвертируем в деревья объектов
            SqlObjectUtils.buildTreesForExpressions(result);
            // специфика реализации дистилляции полнотекстового условия:
            // 1. развернули в набор условий, подлежащий дистилляции
            // 2. сначала заменили исходный объект в дереве, иначе может не выполниться дистилляция
            // 3. только затем дистиллируем созданное
            // TODO вообще говоря дистилляцию полнотекстового условия нужно бы переделать
            DistillerUtils.replace(item, result);
            result = (Predicate)internalDistillate(result, params, state);
        }
        else {
            state.throwError(
                    String.format(
                            "В результате дистиляции предиката полнотекстового поиска получен результат непредусмотренного типа. '%s'",
                            result.getClass().getName()
                    ),
                    item
            );
            result = null;
        }

        return result;
    }

    protected static SqlObject distillatePredicateRegExpMatch(PredicateRegExpMatch item, DistillerParams params, DistillationState state) {
        ColumnExpression expr = (ColumnExpression)internalDistillate(item.getExpr(), params, state);
        ColumnExpression template = (ColumnExpression)internalDistillate(item.getTemplate(), params, state);
        if (expr != null && template != null)
            return item;
        return null;
    }

    protected static SqlObject distillatePredicateInTuple(PredicateInTuple item, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        TupleExpressions tuple = item.getTuple();
        ColumnExpression[] operands = new ColumnExpression[tuple.itemsCount() + 1];
        ColumnExpression[] values = new ColumnExpression[tuple.itemsCount() + 1];
        ColumnExpression expr = item.getExpr();

        if (tuple.itemsCount() == 0) {
            distillated = false;
            state.throwError(
                    String.format(
                            "В условии %s не задан кортеж выражений",
                            item.getClass().getName()
                    ),
                    item
            );
        }

        if (expr == null) {
            distillated = false;
            state.throwError(
                    String.format(
                            "В условии %s не задано выражение",
                            item.getClass().getName()
                    ),
                    item
            );
        }
        else if (expr instanceof ValueConst || expr instanceof ParamRef)
            values[0] = expr;
        else
            operands[0] = expr;

        int i = 1;
        for (SqlObject child: item.getTuple())
            if (child instanceof ValueConst || child instanceof ParamRef)
                values[i++] = (ColumnExpression) child;
            else
                operands[i++] = (ColumnExpression) child;

        ColumnExprTechInfo distInfo = new ColumnExprTechInfo();
        for (ColumnExpression operand: operands) {
            if (operand == null)
                continue;
            ColumnExpression dist = (ColumnExpression)internalDistillate(operand, params, state);
            if (dist != null) {
                ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(dist);
                distInfo.caseSensitive = distInfo.caseSensitive && techInfo.caseSensitive;
                if (techInfo.fieldTypeId != tid_UNKNOWN)
                    if (distInfo.fieldTypeId == tid_UNKNOWN)
                        distInfo.fieldTypeId = techInfo.fieldTypeId;
                    else if (distInfo.fieldTypeId != techInfo.fieldTypeId) {
                        distillated = false;
                        state.throwError(
                                String.format(
                                        "В условии %s операнды разных типов '%s-%s'",
                                        distInfo.fieldTypeId,
                                        techInfo.fieldTypeId),
                                item
                        );
                    }
            }
            else
                distillated = false;
        }

        FieldTypeId back = state.getNeededType();
        try {
            state.setNeededType(distInfo.fieldTypeId);
            for (ColumnExpression value: values)
                if (value != null)
                    distillated = distillated && internalDistillate(value, params, state) != null;
        }
        finally {
            state.setNeededType(back);
        }

        if (distillated) {
            if (!distInfo.caseSensitive) {
                DistillerUtils.applyUpper(item.getExpr(), params);
                for (int j = 0; j < tuple.itemsCount(); j++)
                    DistillerUtils.applyUpper((ColumnExpression) tuple.getItem(j), params);
            }
            return item;
        }
        else
            return null;
    }

    protected static SqlObject distillatePredicateInQuery(PredicateInQuery item, DistillerParams params, DistillationState state) {
        boolean distillated;
        Select select = item.findSelect();
        if (select != null)
            distillated =  internalDistillate(select, params, state) != null;
        else {
            distillated = false;
            state.throwError(
                    String.format(
                            "Для условия '%s' не задан подзапрос",
                            item.getClass().getName()),
                    item
            );
        }

        boolean validItemsCount = select != null  && item.getTuple().itemsCount() == select.getColumns().itemsCount();
        if (!validItemsCount) {
            distillated = false;
            state.throwError(
                    String.format(
                            "В условии '%s' не соответствуют количество элементов (%d) кортежа и колонок %d подзапроса",
                            item.getClass().getName(),
                            item.getTuple().itemsCount(),
                            select.getColumns().itemsCount()
                    ),
                    item
            );
        }

        ColumnExprTechInfo tupleInfo;
        SelectedColumn column;
        ColumnExprTechInfo columnInfo;
        for (int i = 0; i < item.getTuple().itemsCount(); i++) {
            SqlObject child = item.getTuple().getItem(i);
            ColumnExpression dist = (ColumnExpression)internalDistillate(child, params, state);
            if (validItemsCount && dist != null) {
                tupleInfo = SqlObjectUtils.getTechInfo(dist);
                column = select.getColumns().getColumn(i);
                if (column.getColExpr() == null) {
                    distillated = false;
                    state.throwError(
                            String.format(
                                    "В условии '%s' колонка подзапроса не содержит выражение",
                                    item.getClass().getName()
                            ),
                            item
                    );
                }
                else {
                    columnInfo = SqlObjectUtils.getTechInfo(column.getColExpr());
                    if (tupleInfo.fieldTypeId != columnInfo.fieldTypeId &&
                            tupleInfo.fieldTypeId != tid_UNKNOWN && columnInfo.fieldTypeId!= tid_UNKNOWN) {
                        distillated = false;
                        state.throwError(
                                String.format(
                                        "В условии '%s' несовместимые типы данных '%s-%s'",
                                        item.getClass().getName(),
                                        tupleInfo.fieldTypeId,
                                        columnInfo.fieldTypeId
                                ),
                                item
                        );
                    }
                    if (!(tupleInfo.caseSensitive && columnInfo.caseSensitive)) {
                        DistillerUtils.applyUpper(dist, params);
                        DistillerUtils.applyUpper(column.getColExpr(), params);
                    }
                }
            }
        }
        return distillated ? item : null;
    }

    protected static SqlObject distillatePredicateIsNull(PredicateIsNull item, DistillerParams params, DistillationState state) {
        Predicate result = null;
        boolean distillated = internalDistillate(item.getExpr(), params, state) != null;
        boolean isRaw = item.isRaw || params.useStandartNulls;
        if (distillated) {
            ColumnExpression expr = item.getExpr();
            ColumnExprTechInfo techInfo = Preconditions.checkNotNull(SqlObjectUtils.getTechInfo(expr));
            if (!isRaw && expr.getClass() == QualifiedField.class && techInfo.fieldTypeId != tid_UNKNOWN)
                // создадим сравнение квалифицированного поля с нулевым значением
                return DistillerUtils.replace(item, params.dbSupport.nullComparisonTranslate(((QualifiedField) expr).getQName(), techInfo.fieldTypeId, item.not));
            else
                result = item;
        }
        return distillated ? result : null;
    }

    protected static void optimizeBracket(Conditions bracket)
            throws SqlObjectException{
        for (SqlObject child: bracket)
            if (child.getClass() == Conditions.class) {
                Conditions internalBracket = (Conditions)child;
                optimizeBracket(internalBracket);
                if (internalBracket.isEmpty())
                    // пустая скобка - просто удалим
                    bracket.removeItem(internalBracket);
                else if (bracket.booleanOp == internalBracket.booleanOp) {
                    if (!internalBracket.not) {
                        int internalBracketIndex = bracket.indexOf(internalBracket);
                        // вложенная скобка без отрицания - поднимем наверх все вложенные условия
                        // на место содержащей их скобки
                        while (internalBracket.itemsCount() != 0)
                            bracket.insertItem(internalBracket.getItem(0), internalBracketIndex++);
                        // удалим опустевшую вложенную скобку
                        bracket.removeItem(internalBracket);
                    }
                    else if (internalBracket.itemsCount() == 1) {
                        int internalBracketIndex = bracket.indexOf(internalBracket);
                        // вложенная скобка с отрицанием, но условие содержит только одно - поднимем наверх условие
                        Predicate internalPredicate = (Predicate)internalBracket.getItem(0);
                        bracket.insertItem(internalPredicate, internalBracketIndex);
                        internalPredicate.not = internalPredicate.not ^ internalBracket.not;
                        // удалим опустевшую вложенную скобку
                        bracket.removeItem(internalBracket);
                    }
                    else {
                        // оставляем как есть
                    }
                }
                else if (internalBracket.itemsCount() == 1) {
                    // логические операции вложенной и охватывающей скобки не совпадают,
                    // но вложенная содержит только одно условие
                    Predicate internalPredicate = (Predicate)internalBracket.getItem(0);
                    bracket.insertItem(internalPredicate);
                    internalPredicate.not = internalPredicate.not ^ internalBracket.not;
                    // удалим опустевшую вложенную скобку
                    bracket.removeItem(internalBracket);
                }
                else if (bracket.itemsCount() == 1) {
                    // логические операции вложенной и охватывающей скобки не совпадают,
                    // но охватывающая содержит только одно условие - вложенную скобку.
                    // перенесём условия из вложенной скобки в охватывающую
                    // вместе с операцией и отрицанием
                    while (internalBracket.itemsCount() != 0)
                        bracket.insertItem(internalBracket.getItem(0));
                    bracket.booleanOp = internalBracket.booleanOp;
                    bracket.not = bracket.not ^ internalBracket.not;
                    bracket.removeItem(internalBracket);
                }
            }
    }

    protected static SqlObject distillateConditions(Conditions item, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        for (SqlObject child: item)
            distillated = distillated && internalDistillate(child, params, state) != null;
        if (distillated && item.getOwner().getClass() != Conditions.class)
            optimizeBracket(item);
        return distillated ? item : null;
    }

    protected static SqlObject distillateCursorSpecification(CursorSpecification item, DistillerParams params, DistillationState state) {
        state.contextParent = item;

        internalDistillate(item.findSelect(), params, state);
        OrderBy orderBy = item.findOrderBy();
        if (orderBy != null)
            internalDistillate(orderBy, params, state);
        return item;
    }

    protected static boolean tryDistillateUnions(UnionsContainer unions, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        for(SqlObject child: unions) {
            UnionItem union = (UnionItem) child;
            if (union.findSelect() == null) {
                distillated = false;
                state.throwError(
                        String.format(
                                "В элементе '%s' отсутствует запрос",
                                union.getClass().getName()
                        ),
                        union
                );
            }
            distillated = distillated && internalDistillate(union.findSelect(), params, state) != null;
        }
        return distillated;
    }

    protected static SqlObject distillateSelect(Select item, DistillerParams params, DistillationState state) {

        Select backHeadOfUnionsChain = null;
        SqlQuery backParent = null;
        boolean isTopSelectOfUnions = (item.getOwner() == null) || (item.getOwner().getClass() != UnionItem.class);
        if (isTopSelectOfUnions) {
            backHeadOfUnionsChain = state.contextHeadOfUnionsChain;
            state.contextHeadOfUnionsChain = item;
        }
        if (
                state.contextHeadOfUnionsChain.getOwner() == null || // если это корневой Select (даже не содержащийся в Cursor-е)...
                        state.contextHeadOfUnionsChain.getOwner() != state.contextParent // ... или это один из подзапросов (Select, не содержащийся в Cursor-е) - ...
                // вообще говоря, Select может содержаться в UnionItem, который содержится в Select'е, который, в свою очередь - в Cursor-е.
                // но это уже не важно, т.к. дистилляция раздела Cursor.OrderBy будет выполняться
                // с учётом имён полей из первого Select-а в Union-цепочке...
                ) {
            backParent = state.contextParent;
            state.contextParent = item; // ...- назначим текущий Select как задающий пространство имён
        }
        // если же Select содержится в Cursor-е - пространство имён будет задавать именно Cursor

        try {
            // сопоставили текущий select и самый верхний select в union-цепочке
            state.cacheSelectToHeadOfUnionsChainMap.put(item, state.contextHeadOfUnionsChain);

            boolean distillated = true;
        /* Порядок обхода разделов SELECT-а:
          FROM
          WHERE
          GROUP BY
          SELECT (колонки)
          ORDER BY
        */
            CTEsContainer with = item.findWith();
            FromContainer from = item.findFrom();
            Conditions where = item.findWhere();
            GroupBy groupBy = item.findGroupBy();
            SelectedColumnsContainer columns = item.findColumns();
            UnionsContainer unions = item.findUnions();

            if (with != null)
                for (SqlObject cte : with)
                    state.cacheCTEsNotUsedYet.add((CommonTableExpression) cte);

            if (from != null)
                distillated = distillated && distillateFrom(from, params, state) != null;

            if (where != null)
                distillated = distillated && distillateConditions(where, params, state) != null;

            if (groupBy != null)
                distillated = distillated && distillateGroupBy(groupBy, params, state) != null;

            distillated = distillated && distillateColumns(columns, params, state) != null;

            if (unions != null)
                distillated = distillated && tryDistillateUnions(unions, params, state);

            return distillated ? item : null;
        }
        finally {
            if (backParent != null)
                state.contextParent = backParent;
            if (backHeadOfUnionsChain != null)
                state.contextHeadOfUnionsChain = backHeadOfUnionsChain;
        }
    }

    protected static SqlObject distillateInsert(SqlQueryInsert item, DistillerParams params, DistillationState state) {

        state.contextParent = item;

        boolean distillated = true;
        DMLFieldsAssignments assignments = item.getAssignments();

        Select select = item.findSelect();
        if (select != null) {
            // вставка значений из выборки
            state.addResolverFor(item, state.cacheSelectToHeadOfUnionsChainMap);
            select = (Select) internalDistillate(select, params, state);
            distillated = distillated && select != null;
            select = item.findSelect();
            if (assignments.itemsCount() != select.getColumns().itemsCount()) {
                state.throwError(
                        String.format(
                                "В запросе '%s' не соответствуют количество устанавливаемых полей и колонок запроса",
                                item.getClass().getName()),
                        item
                );
                distillated = false;
            }
            for (int i = 0; i < assignments.itemsCount(); i++) {
                DMLFieldAssignment assignment = (DMLFieldAssignment)assignments.getItem(i);
                QualifiedField field = assignment.getField();
                field = (QualifiedField) internalDistillate(field, params, state);
                distillated = distillated && field != null;
                /* Всем объектам QualifiedRField после дистиляции устанавливается алиасная часть - имя/алиас таблицы.
                        Но имена полей, которым присваиваются значения в запросах INSERT/UPDATE,
                        должны быть неквалифицированными, т.е. не должно указываться имя таблицы запроса.
                    Oracle при выполнении запроса понимает квалифицированные имена, а, например, SQLite выдаёт синтаксическую ошибку.
                */
                field.alias = null;
                if (i < select.getColumns().itemsCount()) {
                    ColumnExprTechInfo assignmentTechInfo = assignment.getField().distTechInfo;
                    ColumnExprTechInfo columnTechInfo = select.getColumns().getColumn(i).getColExpr().distTechInfo;
                    if (assignmentTechInfo != null && columnTechInfo != null)
                        if (assignmentTechInfo.fieldTypeId != columnTechInfo.fieldTypeId) {
                            state.throwError(
                                    String.format(
                                            "В запросе '%s' не соответствуют типы устанавливаемого поля '%s' и %d-й колонки запроса",
                                            item.getClass().getName(),
                                            field.fieldName,
                                            i
                                    ),
                                    item
                            );
                            distillated = false;
                        }
                }
            }
        }
        else {
            // простая вставка значений
            state.addResolverFor(item, state.cacheSelectToHeadOfUnionsChainMap);
            for (int i = 0; i < assignments.itemsCount(); i++) {
                DMLFieldAssignment assignment = (DMLFieldAssignment)assignments.getItem(i);
                QualifiedField field = assignment.getField();
                ColumnExpression expr = assignment.getExpr();
                if (field == null) {
                    state.throwError(
                            String.format(
                                    "В запросе '%s' не задано устанавливаемое поле",
                                    item.getClass().getName()),
                            item
                    );
                    distillated = false;
                }
                if (expr == null) {
                    state.throwError(
                            String.format(
                                    "В запросе '%s' не задано присваиваемое выражение",
                                    item.getClass().getName()
                            ),
                            item
                    );
                    distillated = false;
                }
                if (field != null)
                    field = (QualifiedField) internalDistillate(field, params, state);

                FieldTypeId backTypeId = state.getNeededType();
                try {
                    if (field != null) {
                        field.alias = null;
                        state.setNeededType(Utils.getFieldTypeId(field));
                    }
                    expr = (ColumnExpression) internalDistillate(expr, params, state);
                }
                finally {
                    state.setNeededType(backTypeId);
                }
                distillated = distillated && (field != null) && (expr != null);
            }
        }
        return distillated ? item : null;
    }

    protected static SqlObject distillateUpdate(SqlQueryUpdate item, DistillerParams params, DistillationState state) {

        state.contextParent = item;

        boolean distillated = true;
        DMLFieldsAssignments assignments = item.getAssignments();
        state.addResolverFor(item, state.cacheSelectToHeadOfUnionsChainMap);
        for (int i = 0; i < assignments.itemsCount(); i++) {
            DMLFieldAssignment assignment = (DMLFieldAssignment)assignments.getItem(i);
            QualifiedField field = assignment.getField();
            ColumnExpression expr = assignment.getExpr();
            if (field == null) {
                state.throwError(
                        String.format(
                                "В запросе '%s' не задано устанавливаемое поле",
                                item.getClass().getName()
                        ),
                        item
                );
                distillated = false;
            }
            if (expr == null) {
                state.throwError(
                        String.format(
                                "В запросе '%s' не задано присваиваемое выражение",
                                item.getClass().getName()
                        ),
                        item
                );
                distillated = false;
            }
            if (field != null)
                field = (QualifiedField) internalDistillate(field, params, state);

            FieldTypeId backTypeId = state.getNeededType();
            try {
                if (field != null) {
                    field.alias = null;
                    state.setNeededType(Utils.getFieldTypeId(field));
                }
                expr = (ColumnExpression) internalDistillate(expr, params, state);
            }
            finally {
                state.setNeededType(backTypeId);
            }
            distillated = distillated && (field != null) && (expr != null);
        }
        Conditions where = item.findWhere();
        if (where != null)
            distillated = distillated && internalDistillate(where, params, state) != null;
        return distillated ? item : null;
    }

    protected static SqlObject distillateDelete(SqlQueryDelete item, DistillerParams params, DistillationState state) {

        state.contextParent = item;

        Conditions where = item.findWhere();
        if (where == null)
            return item;
        else {
            state.addResolverFor(item, state.cacheSelectToHeadOfUnionsChainMap);
            if (internalDistillate(where, params, state) != null)
              return item;
            return null;
        }
    }

    protected static SqlObject distillateCallStoredProcedure(CallStoredProcedure item, DistillerParams params, DistillationState state) {
        boolean distillated = true;
        TupleExpressions tuple = item.findTuple();
        if (tuple != null)
            for (int i = 0; i < tuple.itemsCount(); i++)
                distillated = distillated && internalDistillate(tuple.getItem(i), params, state) != null;
        return distillated ? item : null;
    }

    protected static SqlObject distillateFrom(FromContainer item, DistillerParams params, DistillationState state) {
        boolean isFirst = true;
        for (SqlObject child: item) {
            FromClauseItem fromItem = (FromClauseItem)child;
            String alias = fromItem.getAlias();
            // сначала, обрабатывая очередной элемент From, расширяем пространство имён запроса, содержащего данный раздел FROM...
            Source source = Preconditions.checkNotNull((fromItem).getTableExpr().getSource());
            if (source instanceof SourceQuery) {
                if (StringUtils.isEmpty(alias)) {
                    state.throwError("Подзапрос в разделе FROM не имеет алиаса", item);
                    return null;
                }
                Select select = ((SourceQuery)source).findSelect();
                if (select == null) {
                    state.throwError(
                            String.format(
                                "В элементе '%s' раздела FROM отсутствует ожидаемый подзапрос",
                                    StringUtils.isEmpty(fromItem.getAliasOrName()) ? "" : fromItem.getAliasOrName()
                            ),
                            item
                    );
                    return null;
                }
                if (internalDistillate(select, params, state) == null)
                    return null;
                state.addResolverFor(((SourceQuery) source).findSelect(), alias,  state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
            }
            else if (source instanceof SourceTable) {
                CommonTableExpression cte = SqlObjectUtils.CTE.findCTE(fromItem);
                String table = ((SourceTable) source).table;
                Preconditions.checkState(!StringUtils.isEmpty(table));
                String name = StringUtils.isEmpty(alias) ? ((SourceTable) source).table : alias;
                if (cte == null) {
                    state.addResolverFor(table, name, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
                }
                else {
                    // сначала добавим cte-запрос в пространство имён,
                    // т.к. он может обращаться к самому себе рекурсивно
                    state.addResolverFor(cte, name, state.contextParent, state.cacheSelectToHeadOfUnionsChainMap);
                    if (internalDistillate(cte, params, state) == null)
                        return null;
                }
            }
            else if (source == null)
                throw new DistillationException("Не указан источник записей в табличном выражении");
            else
                throw new DistillationException(String.format("Источник данных '%s' не поддержан", source.getClass().getName()));
            // ...затем для подлитых элементов выполняем дистилляцию условий, с учётом только что расширенного пространства имён
            if (isFirst)
                // в основной таблице информация о слияниях не нужна - тихо удалим
                fromItem.setJoin(null);
            else {
                Join join = fromItem.getJoin();
                Predicate joinOn = join == null ? null : join.getJoinOn();
                if (joinOn != null && internalDistillate(joinOn, params, state) == null)
                    return null;
            }
            isFirst = false;
        }
        return item;
    }

    protected static boolean checkCTE(CommonTableExpression cte, DistillationState state)
            throws DistillationException {
        if (StringUtils.isEmpty (cte.alias)) {
            state.throwError(
                    "Не задан алиас для cte-запроса",
                    cte
            );
            return false;
        }
        FromContainer from = cte.getSelect().getFrom();
        if (from != null)
            for (SqlObject child: from) {
                Source source = ((FromClauseItem) child).getTableExpr().getSource();
                if (source instanceof SourceTable) {
                    String table = Preconditions.checkNotNull(source.getTable());
                    if (table.equalsIgnoreCase(cte.alias)) {
                        state.throwError(
                                String.format(
                                        "В CTE-запросе '%s' первый SELECT (до UNION) не должен быть рекурсивным",
                                        cte.alias
                                ),
                                cte.getSelect()
                        );
                        return false;
                    }
                }
            }
        return true;
    }

    protected static SqlObject distillateCTE(CommonTableExpression item, DistillerParams params, DistillationState state) {
        if (state.contextDistillatingCTEs.contains(item)) // дистилляция данного cte в процессе...
            return item;
        if (state.cacheCTEsNotUsedYet.contains(item)) { // к CTE ещё не обращались, а значит не дистиллировали...
            // отметим, что обратились...
            state.cacheCTEsNotUsedYet.remove(item);
            state.contextDistillatingCTEs.add(item);
            try {
                if (!checkCTE(item, state))
                    return null;
                /*
                    ANSI не позволяет вложенные CTE - запросы, тем не менее, т.к. sqlobject-запрос может
                    создаваться сложным алгоритмом, частями - в результате это ограничение может нарушаться.
                    Чтобы получить верный запрос - "вытащим" вложенные CTE-запросы, раставив их в запросе в нужном порядке
                */
                if (state.getCurrentCTE() == null)
                    // отметили, что "погрузились" в CTE-запрос первый раз
                    state.setCurrentCTE(item);
                else
                    // этот CTE-запрос оказался вложенным. "поднимем" его, разместив в запросе перед охватывающим его CTE-запросом
                    state.setEarlyThanCurrentCTE(item);
                try {
                    Select select = item.getSelect();
                    if (internalDistillate(select, params, state) == null)
                        return null;
                    if (item.columns.size() != select.getColumns().itemsCount())
                        state.throwError(
                                "В cte-запросе '%s' количество %d перечисленных в заголовке колонок  не соответствует количеству %d колонок его запроса",
                                item
                        );
                } finally {
                    if (state.getCurrentCTE() == item)
                        state.setCurrentCTE(null);
                }
            }
            finally {
                state.contextDistillatingCTEs.remove(item);
            }
        }
        return item;
    }

    protected static SqlObject distillateColumns(SelectedColumnsContainer item, DistillerParams params, DistillationState state) {
        // среди колонок могут быть RecId'ы, и "наши" звёздочки, поэтому соберём сначала в массив
        SqlObject[] childs = item.getSubItems();
        if (childs.length != 0)
            for (SqlObject child : childs) {
                SelectedColumn column = (SelectedColumn) child;
                if (DistillerUtils.columnContainsSpecialQField(column)) {
                    DistillerUtils.checkQFieldUsage((QualifiedField)column.getColExpr(), null);
                    QualifiedField specialQField = (QualifiedField) column.getColExpr();
                    ColumnExprTechInfo[] fields;
                    if (SqlObjectUtils.isRecordId(specialQField)) {
                        // recId может быть взят только из конкретной таблицы, поэтому обозначим табличное выражение
                        specialQField.alias = DistillerUtils.calcQFieldAliasPart(specialQField, state.contextParent);
                        fields = state.getResolver().getRecId(specialQField.alias, SqlObjectUtils.getParentQuery(specialQField), state.cacheSelectToHeadOfUnionsChainMap);
                        if (fields == null || fields.length == 0) {
                            state.throwError(
                                    String.format("Не удалось развернуть в набор полей выражение '[%s].RecId'", specialQField.alias),
                                    item
                            );
                            return null;
                        }
                    }
                    else if (SqlObjectUtils.isAsterisk(specialQField)) {
                        // перечень полей для '*' может быть взят как из определённого табличного выражения, так и собран
                        // по всем таблицам запроса, поэтому имя табличного выражения явно не задаём
                        fields = state.getResolver().getAsteriskFields(specialQField.alias, SqlObjectUtils.getParentQuery(specialQField), state.cacheSelectToHeadOfUnionsChainMap);
                        if (fields == null || fields.length == 0) {
                            state.throwError(
                                    String.format("Не удалось развернуть в набор полей выражение '[%s].*'", specialQField.alias),
                                    item
                            );
                            return null;
                        }
                    }
                    else
                        throw new DistillationException("");

                    // по набору полей создадим замещающие колонки
                    SelectedColumn[] columns = new SelectedColumn[fields.length];
                    for(int i = 0; i < fields.length; i++) {
                        QualifiedField qfield = new QualifiedField(fields[i].tableExprName, fields[i].nativeFieldName);
                        qfield.distTechInfo = fields[i];
                        columns[i] = new SelectedColumn().setExpression(qfield);
                    }
                    item.replaceWithSet(child, columns);
                }
                else
                    // обычную колонку дистиллируем обычным образом
                    internalDistillate(column, params, state);
            }
        else
            state.throwError(
                    "В запросе отсутствуют колонки",
                    item
            );
        return item;
    }

    protected static SqlObject distillateColumn(SelectedColumn item, DistillerParams params, DistillationState state) {
        ColumnExpression expr = item.getColExpr();
        if (expr == null) {
            state.throwError(
                    "Не задано выражение для колонки",
                    item
            );
            return null;
        }
        internalDistillate(expr, params, state);
        Select parent = SqlObjectUtils.getParentSelect(item);
        if (!((parent != null) && (parent.getOwner() == null || parent.getOwner() instanceof CursorSpecification || parent.getOwner() instanceof SourceQuery)))
            // алиасы колонкам нужны только в корневом запросе или подзапросах в разделе From
            item.alias = null;
        return item;
    }

    protected static SqlObject distillateGroupBy(GroupBy item, DistillerParams params, DistillationState state) {
        TupleExpressions tuple = item.getTupleItems();
        if (tuple != null && tuple.isHasChilds()) {
            for (SqlObject child: tuple)
                internalDistillate(child, params, state);
            Conditions having = item.getHaving();
            if (having != null && !having.isEmpty())
                internalDistillate(having, params, state);
        }
        return item;
    }

    protected static SqlObject distillateOrderBy(OrderBy item, DistillerParams params, DistillationState state) {
        for(SqlObject child: item) {
            OrderByItem orderItem = (OrderByItem)child;
            DistillerUtils.applyUpperIfNotCaseSensitive((ColumnExpression) internalDistillate(orderItem.getExpr(), params, state), params);
        }
        return item;
    }
}
