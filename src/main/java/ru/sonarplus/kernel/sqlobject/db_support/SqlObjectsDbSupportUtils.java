package ru.sonarplus.kernel.sqlobject.db_support;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class SqlObjectsDbSupportUtils {
	public static final String EXPR_FUNCTION_UPPER = "expr_upper_sonar";
	public static final String EXPR_FUNCTION_COUNT = "expr_count_sonar";
	public static final String EXPR_FUNCTION_MAX = "expr_max_sonar";
	public static final String EXPR_FUNCTION_MIN = "expr_min_sonar";
	public static final String EXPR_FUNCTION_SUM = "expr_sum_sonar";
	public static final String EXPR_FUNCTION_AVG = "expr_avg_sonar";
	public static final String EXPR_FUNCTION_COALESCE = "expr_coalesce_sonar";
	public static final String EXPR_FUNCTION_ROUND = "expr_round_sonar";
	public static final String EXPR_FUNCTION_YEAR_FROM_DATE = "expr_year_from_date";

	Map<String, Function<String, String>> replacersMap = new HashMap <String, Function<String, String>>();

	public SqlObjectsDbSupportUtils() {
		replacersMap.put(EXPR_FUNCTION_UPPER, this::upper);
		replacersMap.put(EXPR_FUNCTION_COUNT, this::dbCount);
		replacersMap.put(EXPR_FUNCTION_MAX, this::dbMax);
		replacersMap.put(EXPR_FUNCTION_MIN, this::dbMin);
		replacersMap.put(EXPR_FUNCTION_SUM, this::dbSum);
		replacersMap.put(EXPR_FUNCTION_AVG, this::dbAvg);
		replacersMap.put(EXPR_FUNCTION_COALESCE, this::dbCoalesce);
		replacersMap.put(EXPR_FUNCTION_ROUND, this::dbRound);
		replacersMap.put(EXPR_FUNCTION_YEAR_FROM_DATE, this::dbYearFromDate);
	}

	public String dbUpperName() {
		return "UPPER";
	}
	
	public String dbCountName() {
		return "COUNT";
	}

	public String dbMaxName() {
		return "MAX";
	}

	public String dbMinName() {
		return "MIN";
	}

	public String dbSumName() {
		return "SUM";
	}

	public String dbAvgName() {
		return "AVG";
	}

	public String dbCoalesceName() {
		return "COALESCE";
	}

	public String dbRoundName() {
		return "ROUND";
	}
	
	public String dbUpperTemplate() {
		return dbUpperName() + "(%s)";
	}
	
	public String dbCountTemplate() {
		return dbCountName() + "(%s)";
	}

	public String dbMaxTemplate() {
		return dbMaxName() + "(%s)";
	}

	public String dbMinTemplate() {
		return dbMinName() + "(%s)";
	}

	public String dbSumTemplate() {
		return dbSumName() + "(%s)";
	}

	public String dbAvgTemplate() {
		return dbAvgName() + "(%s)";
	}
	
	public String dbCoalesceTemplate() {
		return dbCoalesceName() + "(%s)";
	}

	public String dbRoundTemplate() {
		return dbRoundName() + "(%s)";
	}
	
	public abstract String dbYearFromDateTemplate();
	
	public String upper(String source) {
		return String.format(dbUpperTemplate(), source);
	}
	
	public String dbCount(String source) {
		return String.format(dbCountTemplate(), source);
	}
	
	public String dbMax(String source) {
		return String.format(dbMaxTemplate(), source);
	}
	
	public String dbMin(String source) {
		return String.format(dbMinTemplate(), source);
	}
	
	public String dbSum(String source) {
		return String.format(dbSumTemplate(), source);
	}

	public String dbAvg(String source) {
		return String.format(dbAvgTemplate(), source);
	}

	public String dbCoalesce(String source) {
		return String.format(dbCoalesceTemplate(), source);
	}
	
	public String dbRound(String source) {
		return String.format(dbRoundTemplate(), source);
	}
	
	public String dbYearFromDate(String source) {
		return String.format(dbYearFromDateTemplate(), source);
	}
	
	public boolean tryReplaceFunctionTagWithNativeFunction(Expression expr, String tag, String content) {
		Function<String, String> func = replacersMap.get(tag);
		boolean result = func != null;
		if (result)
			expr.setExpr(func.apply(content));
		return result;
	}
	
	public String subStr(String source, int start, int count) {
		return String.format("SubStr(%s,%d,%d)", source, start, count);
	}
	
	public String expressionStrForValue(Value value) throws SqlObjectException {
		if (value.isNull()) {
			return "NULL";
		}
		else {
            switch (value.getValueType()) {
                case tid_SMALLINT:
                    return value.getValue().toString();
                case tid_INTEGER:
                    return value.getValue().toString();
                case tid_WORD:
                    return value.getValue().toString();
                case tid_FLOAT:
                    return value.getValue().toString();
                case tid_LARGEINT:
                    return value.getValue().toString();
                case tid_STRING:
                    // TODO #BAD# явная завязка на наши нулевые значения.
                    // почему #BAD# - нужно бы, наверное, привязаться опции UseStandartNulls
                    String str = (String) value.getValue();
                    if (StringUtils.isEmpty(str))
                        str = " ";
                    return "'" + str.replaceAll("'", "''") + "'";
                default:
                    throw new SqlObjectException(String.format("Конвертация значения типа '%s' в sql сейчас не поддержана", value.getValueType()));
            }
		}
	}

	public ColumnExpression wrapParamRefToExprInDistillationContext(ParamRef paramRef, FieldTypeId type) {
	    return paramRef;
    }

	public ColumnExpression createColumnExprForParameter(Parameter parameter, FieldTypeId type) {
		return (ColumnExpression) parameter.getClone();
	}
	
	public ColumnExpression createColumnExprForValue(Object value, FieldTypeId type)
			throws SqlObjectException {
		return new ValueConst(value, type);
	}
	
	public Object checkValue(Object value, FieldTypeId type) {
		if (value == null) {
			return value;
		}
		if (type == FieldTypeId.tid_CODE) {
			Preconditions.checkArgument(value instanceof CodeValue);
			byte[] valueArray = ((CodeValue) value).getValue();
			StringBuilder result = new StringBuilder();
			
			for (int i = 0; i < valueArray.length; i++) {
				if ((valueArray[i] == 0) && (i > 0)) {
					return result.toString();
				}
				result.append(String.format("%02X", valueArray[i]));
			}
			return result.toString();
		}
		//TODO реализовать
		return value;
	}

	public ColumnExpression buildByteValueExpr(ColumnExpression value)
			throws SqlObjectException {
		return value;
	}
	
	public abstract Predicate createFTSExpression(FullTextEngine engine, PredicateFullTextSearch fts);

    public abstract ColumnExpression createFTSRangeExpression(FullTextEngine engine, int markerValue)
            throws SqlObjectException;

	public abstract Expression getExpressionForBinaryField(String tableAlias, ColumnExprTechInfo fieldSpec)
			throws SqlObjectException;
	
	protected ColumnExpression createColumnExprForEmptyValue(FieldTypeId type)
			throws ValuesSupport.ValueException {
		switch (type) {
		case tid_SMALLINT:
		case tid_INTEGER:
		case tid_WORD:
		case tid_BYTE:
			return new ValueConst(0, type);
		case tid_LARGEINT:
			return new ValueConst(0L, type);
		case tid_FLOAT:
			return new ValueConst(0.0, type);
		case tid_STRING:
			return new ValueConst(" ", type);
		case tid_BOOLEAN:
			return new ValueConst(false, type);
		case tid_CODE:
			return new ValueConst( new byte[]{0}, type);
		case tid_DATETIME:
		case tid_DATE:
		case tid_TIME:
			return new ValueConst(ValuesSupport.SONAR_ZERO_DATETIME , type);
		default:
			Preconditions.checkArgument(false, "'Создание выражения для \"%s\" не поддержано", type.toString());
			return null;
		}
	}
	
	protected Predicate nullComparison(QualifiedName qLatinName, FieldTypeId type)
			throws SqlObjectException {
		QualifiedField rightField = new QualifiedField(qLatinName.alias, qLatinName.name);
		ColumnExpression expr = createColumnExprForEmptyValue(type);
		return new PredicateComparison(rightField, expr, PredicateComparison.ComparisonOperation.EQUAL);
	}
	
	protected abstract Predicate nullComparisonClob(QualifiedName qLatinName)
			throws SqlObjectException;
    protected abstract Predicate nullComparisonBlob(QualifiedName qLatinName)
            throws SqlObjectException;
	
	public Predicate nullComparisonTranslate(QualifiedName qLatinName, FieldTypeId type, boolean not)
			throws SqlObjectException {
		Predicate result;
		switch (type) {
		case tid_MEMO:
			result = nullComparisonClob(qLatinName);
			break;
		case tid_BLOB:
			result = nullComparisonBlob(qLatinName);
			break;
		default:
			result = nullComparison(qLatinName, type);
			break;
		}
		result.not = not;
		return result;
	}

    private SqlObjectsDBRelativeDateSupport relativeDateSupportInstance = null;

    protected abstract SqlObjectsDBRelativeDateSupport createRelativeDateSupport();

    public SqlObjectsDBRelativeDateSupport relativeDateSupport() {
        if (relativeDateSupportInstance == null)
            relativeDateSupportInstance = createRelativeDateSupport();
        return relativeDateSupportInstance;
    }
}
