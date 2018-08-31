package ru.sonarplus.kernel.sqlobject.db_support_ora;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.BytesUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.db_support.FullTextEngine;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDBRelativeDateSupport;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;
import ru.sonarplus.kernel.sqlobject.db_support_ora.FTSStringParser.FTSExpression;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class SqlObjectsDbSupportUtilsOra extends SqlObjectsDbSupportUtils {
	protected static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

	public SqlObjectsDbSupportUtilsOra() { super(); }

	@Override
	public String dbYearFromDateTemplate() {
		return "EXTRACT(YEAR FROM %s)";
	}
	
	protected boolean isServiceSymbol(String src) {
		String s = src.trim();
		return s.startsWith(Expression.UNNAMED_ARGUMENT_REF) ||
				s.startsWith(Character.toString(Expression.CHAR_BEFORE_PARAMETER)) ||
				s.startsWith(Character.toString(Expression.CHAR_BEFORE_TOKEN));
	}
	
	protected String hexToRaw(String arg) {
		StringBuilder result = new StringBuilder("HEXTORAW(");
		if (isServiceSymbol(arg)) {
			result.append(arg);
		}
		else {
			result.append("'");
			result.append(arg.replaceAll("'", "''"));
			result.append("'");
			
		}
		result.append(')');
		return result.toString();
	}
	
    @Override
    public String expressionStrForValue(Value value) throws SqlObjectException {
        if (value.isNull()) {
            return super.expressionStrForValue(value);
        }
        FieldTypeId valueType = value.getValueType();
        switch (valueType) {
            case tid_BOOLEAN:
                return hexToRaw( (Boolean)value.getValue() ? "01" : "00");	
            case tid_BYTE:
                return hexToRaw(Integer.toHexString((Byte)value.getValue()));
            case tid_CODE:
                return hexToRaw(BytesUtils.bytesToHexString(((CodeValue)value.getValue()).getValue()));
            case tid_DATE:
            case tid_TIME:
            case tid_DATETIME:
				LocalDateTime dateTime;
				switch (valueType) {
					case tid_DATE:
						dateTime = LocalDateTime.of((LocalDate) value.getValue(), ValuesSupport.SONAR_ZERO_TIME);
						break;
					case tid_TIME:
						dateTime = LocalDateTime.of(ValuesSupport.SONAR_ZERO_DATE, (LocalTime) value.getValue());
						break;
					case tid_DATETIME:
						dateTime = (LocalDateTime) value.getValue();
						break;
					default:
						throw new AssertionError();
				}
                return "TO_DATE('" + dateTime.format(DATETIME_FORMAT) + "', 'YYYYMMDD HH24:MI:SS')";
            default:
                return super.expressionStrForValue(value);
		}
	}
	
	protected boolean isRaw(FieldTypeId type) {
		return (type == FieldTypeId.tid_BYTE) || (type == FieldTypeId.tid_BOOLEAN) ||
				(type == FieldTypeId.tid_CODE);
	}

    @Override
    public ColumnExpression wrapParamRefToExprInDistillationContext(ParamRef paramRef, FieldTypeId type) {
        if (isRaw(type)) {
            SqlObject owner = paramRef.getOwner();
            if (owner == null)
                throw new SqlObjectException("");

            if (owner.getClass() == Expression.class) {
                String strExpr = hexToRaw(Expression.UNNAMED_ARGUMENT_REF);
                if (((Expression)owner).getExpr().equalsIgnoreCase(strExpr))
                    return paramRef;
                Expression result = new Expression(null, strExpr, true);
                result.insertItem(paramRef.getClone());
                return result;
            }
            else {
                Expression result = new Expression(null, hexToRaw(Expression.UNNAMED_ARGUMENT_REF), true);
                result.insertItem(paramRef.getClone());
                return result;
            }
        }
        else
            return super.wrapParamRefToExprInDistillationContext(paramRef, type);
    }

	@Override
	public ColumnExpression createColumnExprForParameter(Parameter parameter, FieldTypeId type) {
		if (isRaw(type)) {
			Expression result = new Expression(hexToRaw(Expression.UNNAMED_ARGUMENT_REF), true);
			result.insertItem(parameter.getClone());
			return result;
		}
		else {
			return super.createColumnExprForParameter(parameter, type);
		}
		
	}
	
	protected Value createRawValue(Object value) throws ValuesSupport.ValueException {
		return new ValueConst(value, FieldTypeId.tid_STRING);
	}

    protected Object checkRawValue(Object value, FieldTypeId type) {
	    if (value == null) {
	        return null;
        }
        switch (type) {
            case tid_BOOLEAN:
                return (Boolean) value ? "01" : "00";
            case tid_BYTE:
                if (value instanceof Byte) {
                    return String.format("%02X", value);
                }
                if (value instanceof Integer) {
                    int intValue = (Integer) value;
                    Preconditions.checkArgument(intValue >= 0 && intValue <= 255,
                            "Переполнение при конвертации значения %d в тип BYTE",
                            value);
                    return String.format("%02X", intValue);
                }
				else if (value instanceof Long) {
					long longValue = (Long) value;
					Preconditions.checkArgument(longValue >= 0 && longValue <= 255,
							"Переполнение при конвертации значения %d в тип BYTE",
							value);
					return String.format("%02X", longValue);
				}
                else {
                    Preconditions.checkArgument(false, "Конвертация значения в тип BYTE не реализовано для типа %s",
                            value.getClass().getName());
                }
            default:
                Preconditions.checkArgument(false, "RAW-тип \"%s\" не поддержан",
                        type.toString());
                return null;
        }
    }


    @Override
    public Object checkValue(Object value, FieldTypeId type) {
        // Оракловые BOOLEAN, BYTE приводятся к виду 00/01, FF
        if ((type == FieldTypeId.tid_BYTE) || (type == FieldTypeId.tid_BOOLEAN)) {
            return checkRawValue(value, type);
        }
        else {
            return super.checkValue(value, type);
        }

    }

	
	@Override
	public ColumnExpression createColumnExprForValue(Object value, FieldTypeId type)
			throws SqlObjectException {
		if (isRaw(type)) {
			Expression result = new Expression(hexToRaw(Expression.UNNAMED_ARGUMENT_REF), true);
			result.insertItem(createRawValue(checkValue(value, type)));
			return result;
		}
		else {
			return super.createColumnExprForValue(value, type);
		}
	}
	
	public static String buildExprRef(ColumnExpression expr) {
		if (expr instanceof Scalar) {
			return "(" + Expression.UNNAMED_ARGUMENT_REF + ")";
		}
		else {
			return Expression.UNNAMED_ARGUMENT_REF;
		}
	}
	
	
	@Override
	public ColumnExpression buildByteValueExpr(ColumnExpression value)
			throws SqlObjectException {
		Expression result = new Expression("TO_NUMBER(RAWTOHEX("+buildExprRef(value)+"),'XX')", true);
		value.id = "";
		result.insertItem(value);
		return result;
	}
	
	protected static class TemplateStrContext {
		FieldTypeId valueType;
	}
	
	protected String getTemplateStr(ColumnExpression expr, TemplateStrContext context) {
		Preconditions.checkNotNull(expr, "Выражение для шаблона полнотекстового поиска не задано");
		Preconditions.checkArgument(expr instanceof ValueConst,
				"Тип выражения \"%s\" для шаблона полнотекстового поиска не предусмотрен",
				expr.getClass().getSimpleName());
		context.valueType = FieldTypeId.tid_UNKNOWN;
		if (expr instanceof Value) {
			context.valueType = ((Value) expr).getValueType();
			return (String) ((Value) expr).getValue();
		}
		return "";
	}
	
	protected static Conditions getFTSConditionsTree(FullTextEngine ftsEngine,
			FTSExpression expression, ColumnExpression field, FieldTypeId valueType,
			boolean sortByResult) {
		FTSConditionBuilderOra ftsBuilder = new FTSConditionBuilderOra(ftsEngine, field, valueType,
				sortByResult);
		return ftsBuilder.buildFTSCondition(expression);
	}
	
	protected String getConditionByFTSExpression(FTSExpression ftsExpression, FullTextEngine engine) {
		FTSExpressionConvertorOra ftsConvertor = new FTSExpressionConvertorOra(engine);
		return ftsConvertor.convertFTSExpression(ftsExpression);
	}
	
	protected Predicate internalCreateFTSExpression(FullTextEngine engine, PredicateFullTextSearch fts) {
		TemplateStrContext context = new TemplateStrContext();
		FTSExpression ftsExpression = FTSStringParser.parseFTSString(getTemplateStr(fts.getTemplate(), context));
		if (ftsExpression.isСontainQuotes) {
			return getFTSConditionsTree(engine, ftsExpression, fts.getExpr(), context.valueType,
					fts.sortByResult);
		}
		else {
			ColumnExpression expr = (ColumnExpression) fts.getExpr().getClone();
			expr.id = "";
			return FTSConditionBuilderOra.getContainsComparison(engine, expr, 
					getConditionByFTSExpression(ftsExpression, engine), context.valueType, fts.sortByResult);
		}
		
	}
	
	@Override
	public Predicate createFTSExpression(FullTextEngine engine, PredicateFullTextSearch fts) {
		Predicate result = internalCreateFTSExpression(engine, fts);
		result.not = fts.not;
		return result;
	}

    @Override
    public ColumnExpression createFTSRangeExpression(FullTextEngine engine, int markerValue)
            throws SqlObjectException {
        switch (engine) {
            case ORACLE_TEXT:
                return new OraFTSRange("score(" + String.valueOf(markerValue) + ")");
            case LUCENE:
                return new OraFTSRange("lscore(" + String.valueOf(markerValue) + ")");
            default:
                throw new SqlObjectException(String.format("Не удалось построить выражение score() для %s", engine.toString()));
        }
    }

    protected static class NullComparisonBuilderCommon {

        public static final String SUBSTR_CLOB = "SUBSTR(TO_CHAR(" + Expression.UNNAMED_ARGUMENT_REF + "),1,1)";
        public static final String SUBSTR_BLOB = "DBMS_LOB.SUBSTR(" + Expression.UNNAMED_ARGUMENT_REF + ",1,1)";

        public static Predicate build(QualifiedName qLatinName, String subStrExpr)
                throws SqlObjectException{
            Conditions result = new Conditions(Conditions.BooleanOp.OR);
                new PredicateIsNull(result)
                        .setExpr(new QualifiedField(qLatinName.alias, qLatinName.name));
                Conditions bracketAnd = new Conditions(Conditions.BooleanOp.AND);
                    buildLengthComparison(bracketAnd, qLatinName);
                    buildSpaceComparison(bracketAnd, qLatinName, subStrExpr);
            return result;
        }

        protected static void buildLengthComparison(SqlObject owner, QualifiedName qLatinName)
                throws SqlObjectException {
            Expression exprLength = new Expression("LENGTH("+Expression.UNNAMED_ARGUMENT_REF+ ")", true);
                new QualifiedField(exprLength, qLatinName.alias, qLatinName.name);

            new PredicateComparison(owner,
                    exprLength,
                    new Expression(null,"1", true), PredicateComparison.ComparisonOperation.EQUAL);
        }

        protected static void buildSpaceComparison(SqlObject owner, QualifiedName qLatinName, String subStrExpr)
                throws SqlObjectException {
            Expression exprOnceChar = new Expression(subStrExpr, true);
                new QualifiedField(exprOnceChar, qLatinName.alias, qLatinName.name);
            Expression exprOneSpace = new Expression("' '", true);
            new PredicateComparison(owner, exprOnceChar, exprOneSpace, PredicateComparison.ComparisonOperation.EQUAL);
        }
    }

    @Override
    protected Predicate nullComparisonClob(QualifiedName qLatinName)
            throws SqlObjectException {
        return NullComparisonBuilderCommon.build(qLatinName, NullComparisonBuilderCommon.SUBSTR_CLOB);
    }

    @Override
    protected Predicate nullComparisonBlob(QualifiedName qLatinName)
            throws SqlObjectException{
        return NullComparisonBuilderCommon.build(qLatinName, NullComparisonBuilderCommon.SUBSTR_BLOB);
    }

	@Override
	public Expression getExpressionForBinaryField(String tableAlias, ColumnExprTechInfo fieldSpec)
			throws SqlObjectException {
		Preconditions.checkArgument(fieldSpec.techInfoPrepared);
		Expression result = null;
		if (fieldSpec.fullTextIndex && !StringUtils.isEmpty(fieldSpec.functionNameForIndex)) {
			result = new Expression(fieldSpec.functionNameForIndex + "(" +
					Expression.UNNAMED_ARGUMENT_REF + ")", true);
			new QualifiedField(result, tableAlias, "ROWID");
		}
		else {
			result = new Expression(Expression.UNNAMED_ARGUMENT_REF, true);
			new QualifiedRField(result, tableAlias, fieldSpec.dbdFieldName);
			
		}
		return result;
	}

    @Override
    protected SqlObjectsDBRelativeDateSupport createRelativeDateSupport() {
        return new SqlObjectsDBRelativeDateSupportOra();
    }
}
