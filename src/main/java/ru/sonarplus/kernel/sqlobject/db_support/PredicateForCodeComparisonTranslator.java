package ru.sonarplus.kernel.sqlobject.db_support;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class PredicateForCodeComparisonTranslator {
	protected static final String SUFFIX_ROOT = "_$_ROOT";
	protected static final String SUFFIX_LO = "_$_LO";      
	protected static final String SUFFIX_HI = "_$_HI";      
	protected static final String SUFFIX_LEVEL = "_$_LEVEL";
    // ... для условий "непосредственно подчинено..." с использованием СБК
    protected static final String SUFFIX_CSB1 = "_$_CSB1";
    protected static final String SUFFIX_CSB2 = "_$_CSB2";
	protected static final int CSB_SUBLEVEL_LIMIT = 127;

	public PredicateForCodeComparisonTranslator() {

	}
	
    protected static class TranslationCommon {

        protected static Predicate predicateVassalCommon(PredicateForCodeComparison predicate,
                                                  ColumnExpression rangeLo,
                                                  ColumnExpression rangeHi,
                                                  ColumnExpression level,
                                                  SqlObjectsDbSupportUtils dbSupport) {
            Predicate predicateCodeInRange = predicateCodeInRange(predicate, rangeLo, rangeHi);
            Predicate predicateCodeLevel = predicateCodeLevel(predicate, level, dbSupport);
            Conditions result = new Conditions(null, Conditions.BooleanOp.AND);
            result.insertItem(predicateCodeInRange);
            result.insertItem(predicateCodeLevel);
            return result;
        }

        protected static Predicate predicateCodeInRange(PredicateForCodeComparison predicate, ColumnExpression rangeLo,
                                                        ColumnExpression rangeHi) {
            return new PredicateBetween((ColumnExpression) predicate.getField().getClone(), exprWithSpec(rangeLo),
                    exprWithSpec(rangeHi));
        }

        protected static Predicate predicateCodeLevel(PredicateForCodeComparison predicate, ColumnExpression valueExpr,
                                                      SqlObjectsDbSupportUtils dbSupport) {
            QualifiedField field = predicate.getField();
            String csbFieldName = SqlObjectUtils.getCSBFieldName(field);
            if (!StringUtils.isEmpty(csbFieldName)) {
                ColumnExpression csbExpr = new QualifiedField(null, field.alias, csbFieldName);
                ColumnExpression codeLevelExpr = dbSupport.buildByteValueExpr(csbExpr);
                ColumnExpression codeValueExpr = createValueExpression(Expression.UNNAMED_ARGUMENT_REF + " + 1",
                        valueExpr);
                return new PredicateInTuple(codeLevelExpr).tupleAdd(codeValueExpr).tupleAdd(
                        createValueExpression(CodeValue.Utils.CSB_MAX_VALUE.toString() +  " - "+ Expression.UNNAMED_ARGUMENT_REF,
                                (ColumnExpression)valueExpr.getClone()));
            }
            else {
                return new PredicateComparison(null, exprWithSpec(codeLevelExpression(field)),
                        createValueExpression(Expression.UNNAMED_ARGUMENT_REF + " + 1", valueExpr), PredicateComparison.ComparisonOperation.EQUAL);
            }
        }

        protected static ColumnExpression exprWithSpec(ColumnExpression expr) {
            return exprWithSpec(expr, FieldTypeId.tid_UNKNOWN);
        }

        protected static ColumnExpression exprWithSpec(ColumnExpression expr, FieldTypeId type) {
            SqlObjectUtils.buildTechInfoForExpr(expr, type);
            return expr;
        }

        protected static ColumnExpression createValueExpression(String expression,
                                                         ColumnExpression valueExpr)
                throws SqlObjectException {
            ColumnExpression result = new Expression(expression, true);
            ColumnExpression subItem = valueExpr;
            result.insertItem(subItem);
            return exprWithSpec(result);
        }

        protected static ColumnExpression codeLevelExpression(ColumnExpression expr) {
            Expression result = new Expression("LENGTH("+ Expression.UNNAMED_ARGUMENT_REF + ")/2", true);
            result.insertItem(expr.getClone());
            return result;
        }

        protected static Predicate predicateVassalCommonUsingCSB(PredicateForCodeComparison predicate,
                                                          ColumnExpression rangeLo, ColumnExpression rangeHi,
                                                          ColumnExpression csbLo, ColumnExpression csbHi) {
            Conditions result = new Conditions(Conditions.BooleanOp.AND);
            result.insertItem(predicateCodeInRange(predicate, rangeLo, rangeHi));
            result.insertItem(predicateCodeLevelUsingCSB(predicate, csbLo, csbHi));
            return result;
        }

        protected static Predicate predicateCodeLevelUsingCSB(PredicateForCodeComparison predicate,
                                                       ColumnExpression csbLoExpr, ColumnExpression csbHiExpr)
                throws SqlObjectException {
            PredicateInTuple result = new PredicateInTuple();
            QualifiedField field = predicate.getField();

            result.setExpr(new QualifiedField(field.alias,
                    SqlObjectUtils.getTechInfo(field).csbNativeFieldName));
            TupleExpressions tuple = result.newTuple();
            tuple.insertItem(csbLoExpr);
            tuple.insertItem(csbHiExpr);
            return result;
        }

        protected static Predicate predicateCodeLeafCommon(PredicateForCodeComparison predicate, ColumnExpression rangeLo, ColumnExpression rangeHi,
                                                           SqlObjectsDbSupportUtils dbSupport) {
            Conditions result = new Conditions(Conditions.BooleanOp.AND);
            result.insertItem(predicateCodeInRange(predicate, rangeLo, rangeHi));
            result.insertItem(predicateLeaf(predicate, dbSupport));
            return result;
        }

        protected static Predicate predicateLeaf(PredicateForCodeComparison predicate, SqlObjectsDbSupportUtils dbSupport) {
            QualifiedField field = predicate.getField();
            ColumnExprTechInfo codeFieldTechInfo = SqlObjectUtils.getTechInfo(field);
            if (!StringUtils.isEmpty(codeFieldTechInfo.csbNativeFieldName)) {
                PredicateComparison result = new PredicateComparison();
                result.comparison = PredicateComparison.ComparisonOperation.LESS_EQUAL;
                result.setLeft(new QualifiedField(null, field.alias, codeFieldTechInfo.csbNativeFieldName));
                result.setRight(dbSupport.createColumnExprForValue(CSB_SUBLEVEL_LIMIT, FieldTypeId.tid_BYTE));
                return result;
            }
            else {
                PredicateExists result = new PredicateExists(null);
                result.not = true;
                Select select = new Select(null);
                select.newFrom().addTable(codeFieldTechInfo.originTableName, "");
                Expression columnExpr = new Expression(null, "1", true);
                select.newColumns().addColumn(columnExpr, "");
                Conditions where = select.newWhere();

                PredicateComparison cmp = new PredicateComparison(where);
                cmp.comparison = PredicateComparison.ComparisonOperation.GREAT;
                QualifiedField left = (QualifiedField) field.getClone();
                left.alias = SqlObjectUtils.getRequestTableName(select);
                cmp.setLeft(left);
                QualifiedField right = (QualifiedField) field.getClone();
                cmp.setRight(right);

                cmp = new PredicateComparison(where);
                cmp.comparison = PredicateComparison.ComparisonOperation.LESS_EQUAL;
                left = (QualifiedField) field.getClone();
                left.alias = SqlObjectUtils.getRequestTableName(select);
                cmp.setLeft(left);
                cmp.setRight(exprConcatWithMaxCode(predicate));
                result.setSelect(select);
                return result;
            }
        }

        protected static ColumnExpression exprConcatWithMaxCode(PredicateForCodeComparison predicate) {
            ColumnExpression result = new Expression(Expression.UNNAMED_ARGUMENT_REF + " || " + Expression.UNNAMED_ARGUMENT_REF, true);
            result = exprWithSpec(result);
            ColumnExpression sourceField = predicate.getField();
            ColumnExpression field = (ColumnExpression) sourceField.getClone();
            result.insertItem(field);
            new Expression(result, maxCodeStr(SqlObjectUtils.getBytesForCode(sourceField)), true);
            return result;
        }

        protected static String maxCodeStr(int size) {
            StringBuilder result = new StringBuilder("'");
            for (int i = 1; i <= size*2; i++) {
                result.append('F');
            }
            result.append("'");
            return result.toString();
        }

        protected static Predicate predicateCodeRootCommon(PredicateForCodeComparison predicate, ColumnExpression root) {
            return new PredicateComparison(null, (QualifiedField) predicate.getField().getClone(),
                    exprWithSpec(root), PredicateComparison.ComparisonOperation.EQUAL);
        }

    }

    protected static class TranslationValues extends TranslationCommon {

        public static Predicate translate(PredicateForCodeComparison predicate, SqlObjectsDbSupportUtils dbSupport) {
            ColumnExpression code = predicate.getCode();
            CodeValue codeRoot = (CodeValue) ((Value) code).getValue();
            if (codeRoot == null) {
                // Если в кодификаторном условии значение кода оказалось равным NULL - как быть?
                // Сейчас формально создаю условие сравнения с NULL, но может быть кодификаторное условие нужно удалять?
                return new PredicateComparison(predicate.getField(), code, PredicateComparison.ComparisonOperation.EQUAL).setNot(predicate.not);
            }

            String csbFieldName = SqlObjectUtils.getTechInfo(predicate.getField()).csbNativeFieldName;
            CodeValue codeRangeLo = CodeValue.Utils.codeRangeLo(codeRoot, code.distTechInfo.bytesForCode);
            CodeValue codeRangeHi = CodeValue.Utils.codeRangeHi(codeRoot, code.distTechInfo.bytesForCode);

            Predicate result;
            switch (predicate.comparison) {
                case ROOT_VASSAL:
                    if (StringUtils.isEmpty(csbFieldName))
                        // При отсутствии СБК в условие between для оптимизации будет включён корневой элемент
                        result = predicateVassal(predicate, codeRoot, codeRangeHi, CodeValue.Utils.codeLevel(codeRoot), dbSupport);
                    else {
                        // ...в противном случае корневой элемент придётся выбирать отдельно по OR (ниже)
                        result = predicateVassalUsingCSB(predicate, codeRangeLo, codeRangeHi,
                                (byte) ((CodeValue.Utils.codeLevel(codeRoot) + 1) & 0xFF),
                                (byte) (254 - CodeValue.Utils.codeLevel(codeRoot) & 0xFF),
                                dbSupport);
                        // корень будет включён отдельным условием
                    }
                    break;

                case CODE_VASSAL:
                    if (StringUtils.isEmpty(csbFieldName))
                        result = predicateVassal(predicate, codeRangeLo, codeRangeHi, CodeValue.Utils.codeLevel(codeRoot), dbSupport);
                    else
                        result = predicateVassalUsingCSB(predicate, codeRangeLo, codeRangeHi,
                                (byte) ((CodeValue.Utils.codeLevel(codeRoot) + 1) & 0xFF),
                                (byte) (254 - CodeValue.Utils.codeLevel(codeRoot) & 0xFF),
                                dbSupport);
                    break;

                case ROOT_ALL:
                    result = predicateAll(predicate, codeRoot, codeRangeHi, dbSupport);
                    break;

                case CODE_ALL:
                    result = predicateAll(predicate, codeRangeLo, codeRangeHi, dbSupport);
                    break;

                case ROOT_LEAF:
                    result = predicateCodeLeaf(predicate, codeRangeLo, codeRangeHi, dbSupport);
                    // корень будет включён отдельным условием
                    break;

                case CODE_LEAF:
                    result = predicateCodeLeaf(predicate, codeRangeLo, codeRangeHi, dbSupport);
                    break;

                default:
                    throw new SqlObjectException(
                            String.format("Кодификаторное условие %s не поддержано", predicate.comparison.toString()));
            }

            // при трансляции условия "непосредственно подчинено или равно" с использованием СБК
            // или "лист или равно" (вне зависимости от СБК)
            // нужно отдельно учесть корневой элемент
            if (
                    (predicate.comparison == PredicateForCodeComparison.ComparisonCodeOperation.ROOT_VASSAL && !StringUtils.isEmpty(csbFieldName)) ||
                            (predicate.comparison == PredicateForCodeComparison.ComparisonCodeOperation.ROOT_LEAF)
                    )
                result  = includeRoot(predicate, result, codeRoot, dbSupport);

            result.not = predicate.not;
            return result;

        }

        protected static Conditions includeRoot(PredicateForCodeComparison predicate, Predicate newConditions, CodeValue codeRoot,
                                                SqlObjectsDbSupportUtils dbSupport) {
            Conditions result = new Conditions(Conditions.BooleanOp.OR);
            result.insertItem(newConditions);
            result.insertItem(predicateCodeRoot(predicate, codeRoot, dbSupport));
            return result;
        }

        protected static Predicate predicateCodeRoot(PredicateForCodeComparison predicate, CodeValue value,
                                                     SqlObjectsDbSupportUtils dbSupport) {
            return predicateCodeRootCommon(predicate, dbSupport.createColumnExprForValue(value, FieldTypeId.tid_CODE));
        }

        protected  static Predicate predicateVassal(PredicateForCodeComparison predicate,
                                                    CodeValue rangeLo, CodeValue rangeHi, Integer level,
                                                    SqlObjectsDbSupportUtils dbSupport) {
            return predicateVassalCommon(predicate, dbSupport.createColumnExprForValue(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForValue(rangeHi, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForValue(level, FieldTypeId.tid_INTEGER),
                    dbSupport
            );
        }

        protected static Predicate predicateVassalUsingCSB(PredicateForCodeComparison predicate,
                                                    CodeValue rangeLo, CodeValue rangeHi,
                                                    byte csbLo,
                                                    byte csbHi,
                                                    SqlObjectsDbSupportUtils dbSupport) {
            return predicateVassalCommonUsingCSB(predicate, dbSupport.createColumnExprForValue(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForValue(rangeHi, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForValue(csbLo, FieldTypeId.tid_BYTE),
                    dbSupport.createColumnExprForValue(csbHi, FieldTypeId.tid_BYTE));
        }

        protected static Predicate predicateAll(PredicateForCodeComparison predicate, CodeValue rangeLo, CodeValue rangeHi,
                                         SqlObjectsDbSupportUtils dbSupport) {
            return predicateCodeInRange(predicate, dbSupport.createColumnExprForValue(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForValue(rangeHi, FieldTypeId.tid_CODE));
        }

        protected static Predicate predicateCodeLeaf(PredicateForCodeComparison predicate, CodeValue rangeLo, CodeValue rangeHi,
                                                     SqlObjectsDbSupportUtils dbSupport) {
            return predicateCodeLeafCommon(predicate, dbSupport.createColumnExprForValue(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForValue(rangeHi, FieldTypeId.tid_CODE), dbSupport);
        }
    }

    protected static class TranslationParams extends TranslationCommon {

        protected static class TranslateContext {
            public QueryParam qParamRoot;
            public QueryParam qParamLo;
            public QueryParam qParamHi;
            public QueryParam qParamLevel;
            public QueryParam qParamCsb1;
            public QueryParam qParamCsb2;
            public ParamRef paramRoot;
            public ParamRef paramLo;
            public ParamRef paramHi;
            public ParamRef paramLevel;
            public ParamRef paramCsb1;
            public ParamRef paramCsb2;
        }

        public static Predicate translate(PredicateForCodeComparison predicate, SqlObjectsDbSupportUtils dbSupport) {
            String csbFieldName = Preconditions.checkNotNull(predicate.getField().distTechInfo).csbNativeFieldName;
            TranslateContext context = new TranslateContext();
            prepareParams(predicate, context);
            Predicate result;
            switch (predicate.comparison) {
                case ROOT_VASSAL:
                    if (StringUtils.isEmpty(csbFieldName)) {
                        // При отсутствии СБК в условие between для оптимизации будет включён корневой элемент
                        result = predicateVassal(predicate, context.paramRoot, context.paramHi, context.paramLevel, dbSupport);
                        context.qParamLo.removeFromOwner();
                    }
                    else {
                        // ...в противном случае корневой элемент придётся выбирать отдельно по OR (ниже)
                        result = predicateVassalUsingCSB(predicate,
                                context.paramLo, context.paramHi, context.paramCsb1, context.paramCsb2,
                                dbSupport);
                        // корень будет включён отдельным условием
                    }
                    break;

                case CODE_VASSAL:
                    if (StringUtils.isEmpty(csbFieldName))
                        result = predicateVassal(predicate, context.paramLo, context.paramHi, context.paramLevel, dbSupport);
                    else
                        result = predicateVassalUsingCSB(predicate,
                                context.paramLo, context.paramHi, context.paramCsb1, context.paramCsb2,
                                dbSupport);
                    context.qParamRoot.removeFromOwner();
                    break;

                case ROOT_ALL:
                    result = predicateAll(predicate, context.paramRoot, context.paramHi, dbSupport);
                    context.qParamLo.removeFromOwner();
                    break;

                case CODE_ALL:
                    result = predicateAll(predicate, context.paramLo, context.paramHi, dbSupport);
                    context.qParamRoot.removeFromOwner();
                    break;

                case ROOT_LEAF:
                    result = predicateCodeLeaf(predicate, context.paramLo, context.paramHi, dbSupport);
                    // корень будет включён отдельным условием
                    break;

                case CODE_LEAF:
                    result = predicateCodeLeaf(predicate, context.paramLo, context.paramHi, dbSupport);
                    context.qParamRoot.removeFromOwner();
                    break;

                default:
                    throw new SqlObjectException(
                            String.format("Кодификаторное условие %s не поддержано", predicate.comparison.toString()));
            }

            // при трансляции условия "непосредственно подчинено или равно" с использованием СБК
            // или "лист или равно" (вне зависимости от СБК)
            // нужно отдельно учесть корневой элемент
            if (
                    (predicate.comparison == PredicateForCodeComparison.ComparisonCodeOperation.ROOT_VASSAL && !StringUtils.isEmpty(csbFieldName)) ||
                            (predicate.comparison == PredicateForCodeComparison.ComparisonCodeOperation.ROOT_LEAF)
                    )
                result = includeRoot(predicate, result, context.paramRoot, dbSupport);

            result.not = predicate.not;
            return result;
        }

        protected static Conditions includeRoot(PredicateForCodeComparison predicate, Predicate newConditions, ParamRef codeRoot,
                                                SqlObjectsDbSupportUtils dbSupport) {
            Conditions result = new Conditions(Conditions.BooleanOp.OR);
            result.insertItem(newConditions);
            result.insertItem(predicateCodeRoot(predicate, codeRoot, dbSupport));
            return result;
        }

        protected static void prepareParams(PredicateForCodeComparison predicate, TranslateContext context)
                throws SqlObjectException {
            ParamRef paramOrg = (ParamRef) predicate.getCode();
            QueryParams paramsClause = SqlObjectUtils.getRootQuery(predicate).newParams();

            String codeRootName = paramOrg.parameterName + SUFFIX_ROOT;
            String codeLoName = paramOrg.parameterName + SUFFIX_LO;
            String codeHiName = paramOrg.parameterName + SUFFIX_HI;
            String codeLevelName = paramOrg.parameterName + SUFFIX_LEVEL;
            String codeCsb1Name = paramOrg.parameterName + SUFFIX_CSB1;
            String codeCsb2Name = paramOrg.parameterName + SUFFIX_CSB2;

            CodeValue codeRoot = getParamCodeValue(paramsClause, paramOrg.parameterName);

            if (paramsClause.findParam(codeRootName) == null) {
                ValueConst valueRoot = new ValueConst(codeRoot, FieldTypeId.tid_CODE);
                context.qParamRoot = new QueryParam(paramsClause, codeRootName, valueRoot, QueryParam.ParamType.INPUT);
            }

            if (paramsClause.findParam(codeLoName) == null) {
                ValueConst valueLo = new ValueConst(CodeValue.Utils.codeRangeLo(codeRoot, predicate.getCode().distTechInfo.bytesForCode), FieldTypeId.tid_CODE);
                context.qParamLo = new QueryParam(paramsClause, codeLoName, valueLo, QueryParam.ParamType.INPUT);
            }

            if (paramsClause.findParam(codeHiName) == null) {
                ValueConst valueHi = new ValueConst(CodeValue.Utils.codeRangeHi(codeRoot, predicate.getCode().distTechInfo.bytesForCode), FieldTypeId.tid_CODE);
                context.qParamHi = new QueryParam(paramsClause, codeHiName, valueHi, QueryParam.ParamType.INPUT);
            }

            if (paramsClause.findParam(codeLevelName) == null) {
                ValueConst valueLevel = new ValueConst(CodeValue.Utils.codeLevel(codeRoot), FieldTypeId.tid_INTEGER);
                context.qParamLevel = new QueryParam(paramsClause, codeLevelName, valueLevel, QueryParam.ParamType.INPUT);
            }

            if (paramsClause.findParam(codeCsb1Name) == null) {
                ValueConst valueLevel = new ValueConst(CodeValue.Utils.codeCSB1(codeRoot), FieldTypeId.tid_BYTE);
                context.qParamCsb1 = new QueryParam(paramsClause, codeCsb1Name, valueLevel, QueryParam.ParamType.INPUT);
            }

            if (paramsClause.findParam(codeCsb2Name) == null) {
                ValueConst valueLevel = new ValueConst(CodeValue.Utils.codeCSB2(codeRoot), FieldTypeId.tid_BYTE);
                context.qParamCsb2 = new QueryParam(paramsClause, codeCsb2Name, valueLevel, QueryParam.ParamType.INPUT);
            }

            context.paramRoot = new ParamRef(codeRootName);
            context.paramLo = new ParamRef(codeLoName);
            context.paramHi = new ParamRef(codeHiName);
            context.paramLevel = new ParamRef(codeLevelName);
            context.paramCsb1 = new ParamRef(codeCsb1Name);
            context.paramCsb2 = new ParamRef(codeCsb2Name);
        }

        protected static CodeValue getParamCodeValue(QueryParams params, String paramName) {
            return (CodeValue)params.findExistingParam(paramName).getValue();
        }

        protected static ColumnExpression createParamExpr(ParamRef expr, FieldTypeId type,
                                                          SqlObjectsDbSupportUtils dbSupport) {
            return dbSupport.createColumnExprForParameter(expr, type);
        }

        protected static Predicate predicateVassal(PredicateForCodeComparison predicate,
                                             ParamRef rangeLo, ParamRef rangeHi, ParamRef level,
                                                   SqlObjectsDbSupportUtils dbSupport) {
            return predicateVassalCommon(predicate, dbSupport.createColumnExprForParameter(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForParameter(rangeHi, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForParameter(level, FieldTypeId.tid_INTEGER),
                    dbSupport
            );
        }

        protected static Predicate predicateVassalUsingCSB(PredicateForCodeComparison predicate,
                                                    ParamRef rangeLo, ParamRef rangeHi, ParamRef csbLo, ParamRef csbHi,
                                                           SqlObjectsDbSupportUtils dbSupport) {
            ColumnExpression exprRangeLo = createParamExpr(rangeLo, FieldTypeId.tid_CODE, dbSupport);
            ColumnExpression exprRangeHi = createParamExpr(rangeHi, FieldTypeId.tid_CODE, dbSupport);

            ColumnExpression exprCsbLo = createParamExpr(csbLo, FieldTypeId.tid_BYTE, dbSupport);
            ColumnExpression exprCsbHi = createParamExpr(csbHi, FieldTypeId.tid_BYTE, dbSupport);
            return predicateVassalCommonUsingCSB(predicate, exprRangeLo, exprRangeHi, exprCsbLo, exprCsbHi);
        }

        protected static Predicate predicateAll(PredicateForCodeComparison predicate, ParamRef rangeLo, ParamRef rangeHi,
                                                SqlObjectsDbSupportUtils dbSupport) {
            return predicateCodeInRange(predicate, dbSupport.createColumnExprForParameter(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForParameter(rangeHi, FieldTypeId.tid_CODE));
        }

        protected static Predicate predicateCodeLeaf(PredicateForCodeComparison predicate, ParamRef rangeLo, ParamRef rangeHi,
                                                     SqlObjectsDbSupportUtils dbSupport) {
            return predicateCodeLeafCommon(predicate, dbSupport.createColumnExprForParameter(rangeLo, FieldTypeId.tid_CODE),
                    dbSupport.createColumnExprForParameter(rangeHi, FieldTypeId.tid_CODE),
                    dbSupport);
        }

        protected static Predicate predicateCodeRoot(PredicateForCodeComparison predicate, ParamRef root,
                                                     SqlObjectsDbSupportUtils dbSupport) {
            return predicateCodeRootCommon(predicate, dbSupport.createColumnExprForParameter(root, FieldTypeId.tid_CODE));
        }

    }

    static public Predicate translate(PredicateForCodeComparison predicate, SqlObjectsDbSupportUtils dbSupport) {
		ColumnExpression code = predicate.getCode();
		Preconditions.checkNotNull(code);
		if (code instanceof Parameter)
		    return TranslationParams.translate(predicate, dbSupport);
		else if (code instanceof ValueConst)
		    return TranslationValues.translate(predicate, dbSupport);
		else {
			Preconditions.checkArgument(false, "Не реализована трансляция для класса " + code.getClass().getSimpleName());
			return null;
		}
		
		
	}
	
}
