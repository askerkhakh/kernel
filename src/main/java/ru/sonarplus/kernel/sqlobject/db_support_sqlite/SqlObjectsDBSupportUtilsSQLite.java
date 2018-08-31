package ru.sonarplus.kernel.sqlobject.db_support_sqlite;

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
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.time.LocalDateTime;

public class SqlObjectsDBSupportUtilsSQLite extends SqlObjectsDbSupportUtils {

    public SqlObjectsDBSupportUtilsSQLite() { super(); }

    @Override
    public Object checkValue(Object value, FieldTypeId type) {

        if (type == FieldTypeId.tid_DATE || type == FieldTypeId.tid_TIME || type == FieldTypeId.tid_DATETIME)
            // дату-время => unixtime
            return ValuesSupport.dateTimeToUnixTime(value);
        else
            return super.checkValue(value, type);
    }

    @Override
    public String expressionStrForValue(Value value) throws SqlObjectException {
        if (value.isNull()) {
            return super.expressionStrForValue(value);
        }
        switch (value.getValueType()) {
            case tid_BOOLEAN:
                return (Boolean)value.getValue() ? "1" : "0";

            case tid_BYTE:
                return Integer.toString((int)((Byte)value.getValue()) & 0xFF);

            case tid_CODE:
                return '\'' + BytesUtils.bytesToHexString(((CodeValue)value.getValue()).getValue()) + '\'';

            case tid_DATE:
            case tid_TIME:
            case tid_DATETIME:
                return Long.toString(ValuesSupport.dateTimeToUnixTime(value.getValue()));
            default:
                return super.expressionStrForValue(value);
        }
    }

    protected Predicate nullComparisonCommon(QualifiedName qLatinName)
            throws SqlObjectException{
        // field is null or (length(field)=1 and substr(field, 1, 1)= ' ')
        Conditions result = new Conditions(Conditions.BooleanOp.OR);
            new PredicateIsNull(result)
                    .setExpr(new QualifiedField(qLatinName.alias, qLatinName.name));

            Conditions bracketAnd = new Conditions(Conditions.BooleanOp.AND);

                Expression exprLength = new Expression("LENGTH("+Expression.UNNAMED_ARGUMENT_REF+ ")", true);
                    exprLength.insertItem(new QualifiedField(qLatinName.alias, qLatinName.name));
                new PredicateComparison(bracketAnd, exprLength, new Expression(null,"1", true), PredicateComparison.ComparisonOperation.EQUAL);

                Expression exprFirstChar = new Expression("SUBSTR(" + Expression.UNNAMED_ARGUMENT_REF + ")", true);
                    exprFirstChar.insertItem(new QualifiedField(qLatinName.alias, qLatinName.name));
                new PredicateComparison(bracketAnd, exprFirstChar, new Expression("' '", true), PredicateComparison.ComparisonOperation.EQUAL);

        return result;
    }

    @Override
    protected Predicate nullComparisonClob(QualifiedName qLatinName)
            throws SqlObjectException {
        return nullComparisonCommon(qLatinName);
    }

    @Override
    protected Predicate nullComparisonBlob(QualifiedName qLatinName)
            throws SqlObjectException{
        return nullComparisonCommon(qLatinName);
    }

    @Override
    public String dbYearFromDateTemplate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Predicate createFTSExpression(FullTextEngine engine, PredicateFullTextSearch fts) { throw new UnsupportedOperationException(); }

    @Override
    public ColumnExpression createFTSRangeExpression(FullTextEngine engine, int markerValue) { throw new UnsupportedOperationException(); }

    @Override
    public Expression getExpressionForBinaryField(String tableAlias, ColumnExprTechInfo fieldSpec) { throw new UnsupportedOperationException(); }

    @Override
    protected SqlObjectsDBRelativeDateSupport createRelativeDateSupport(){
        return new SqlObjectsDBRelativeDateSupportSQLite();
    }

}
