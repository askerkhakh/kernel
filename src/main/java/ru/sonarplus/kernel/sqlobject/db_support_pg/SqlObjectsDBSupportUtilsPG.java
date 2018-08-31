package ru.sonarplus.kernel.sqlobject.db_support_pg;

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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class SqlObjectsDBSupportUtilsPG extends SqlObjectsDbSupportUtils {

    public SqlObjectsDBSupportUtilsPG() { super(); }

    @Override
    public String expressionStrForValue(Value value) throws SqlObjectException {
        if (value.isNull()) {
            return super.expressionStrForValue(value);
        }
        switch (value.getValueType()) {
            case tid_BOOLEAN:
                return (Boolean)value.getValue() ? "True" : "False";

            case tid_BYTE:
                return Integer.toString(((Byte)value.getValue()) & 0xFF);

            case tid_CODE:
                return '\'' + BytesUtils.bytesToHexString(((CodeValue)value.getValue()).getValue()) + '\'';

            case tid_DATE:
                return "DATE'" + ((LocalDate) value.getValue()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + '\'';
            case tid_TIME:
                return "TIME'" + ((LocalTime) value.getValue()).format(DateTimeFormatter.ofPattern("HH:mm:ss")) + '\'';

            default:
                return super.expressionStrForValue(value);
        }
    }

    @Override
    protected Predicate nullComparisonClob(QualifiedName qLatinName)
            throws SqlObjectException {
        PredicateIsNull result = new PredicateIsNull();
        result.setExpr(new QualifiedField(qLatinName.alias, qLatinName.name));
        return result;
    }

    @Override
    protected Predicate nullComparisonBlob(QualifiedName qLatinName)
            throws SqlObjectException{
        PredicateIsNull result = new PredicateIsNull();
        result.setExpr(new QualifiedField(qLatinName.alias, qLatinName.name));
        return result;
    }

    @Override
    public String dbYearFromDateTemplate() {
        return "EXTRACT(YEAR FROM %s)";
    }

    @Override
    public Predicate createFTSExpression(FullTextEngine engine, PredicateFullTextSearch fts) { throw new UnsupportedOperationException(); }

    @Override
    public ColumnExpression createFTSRangeExpression(FullTextEngine engine, int markerValue) { throw new UnsupportedOperationException(); }

    @Override
    public Expression getExpressionForBinaryField(String tableAlias, ColumnExprTechInfo fieldSpec) { throw new UnsupportedOperationException(); }

    @Override
    protected SqlObjectsDBRelativeDateSupport createRelativeDateSupport(){
        return new SqlObjectsDBRelativeDateSupportPG();
    }

}
