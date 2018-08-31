package ru.sonarplus.kernel.sqlobject.db_support_pg;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDBRelativeDateSupport;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.objects.Expression;

public class SqlObjectsDBRelativeDateSupportPG extends SqlObjectsDBRelativeDateSupport {

    public SqlObjectsDBRelativeDateSupportPG() { super(); }

    static final String SQL_START_YEAR = "/*<start of year>*/DATE(DATE_TRUNC('YEAR', %s + INTERVAL '1 YEAR' ) - INTERVAL '1 YEAR')/*</start of year>*/";
    static final String SQL_END_YEAR = "/*<end of year>*/DATE(DATE_TRUNC('YEAR', %s + INTERVAL '1 YEAR' ) - INTERVAL '1 DAY')/*</end of year>*/";
    static final String SQL_START_MONTH = "/*<start of month>*/DATE(DATE_TRUNC('MONTH', %s + INTERVAL '1 MONTH' ) - INTERVAL '1 MONTH')/*</start of month>*/";
    static final String SQL_END_MONTH = "/*<end of month>*/DATE(DATE_TRUNC('MONTH', %s + INTERVAL '1 MONTH' ) - INTERVAL '1 DAY')/*<end of month>*/";
    static final String SQL_START_WEEK = "/*<start of week>*/DATE(DATE_TRUNC('WEEK', %s))/*</start of week>*/";
    static final String SQL_END_WEEK = "/*<end of week>*/DATE(DATE_TRUNC('WEEK', %s + INTERVAL '1 WEEK')- INTERVAL '1 DAY')/*<end of week>*/";
    static final String SQL_START_QUART = "/*<start of quart>*/DATE(DATE_TRUNC('QUARTER', %s))/*</start of quart>*/";
    static final String SQL_END_QUART = "/*<end of quart>*/DATE(DATE_TRUNC('QUARTER', %s + INTERVAL '3 MONTHS') - INTERVAL '1 DAY')/*</end of quart>*/";

    static final String SQL_NEXT_DAY = "DATE(%s + INTERVAL '1 DAY')";

    static final String SQL_OFFSET_YEARS = "DATE(%s + (%s) * INTERVAL '1 YEAR')";
    static final String SQL_OFFSET_MONTHS = "DATE(%s + (%s) * INTERVAL '1 MONTH')";
    static final String SQL_OFFSET_DAYS = "DATE(%s + (%s) * INTERVAL '1 DAY')";
    static final String SQL_OFFSET_QUARTS = "DATE(now() + (%s) * INTERVAL '3 MONTH')";
    static final String SQL_OFFSET_WEEKS = "DATE(now() + (%s) * INTERVAL '1 WEEK')";;

    @Override
    protected String internalTranslateToday(){
        return "DATE(NOW())";
    }

    @Override
    protected String internalTranslateCurrentTime(){
        return "LOCALTIME";
    }

    @Override
    protected String internalTranslateYearStart(Expression expr, String content){
        return String.format(SQL_START_YEAR, content);
    }

    @Override
    protected String internalTranslateYearEnd(Expression expr, String content){
        return String.format(SQL_END_YEAR, content);
    }

    @Override
    protected String internalTranslateMonthStart(Expression expr, String content) {
        return String.format(SQL_START_MONTH, content);
    }

    @Override
    protected String internalTranslateMonthEnd(Expression expr, String content){
        return String.format(SQL_END_MONTH, content);
    }

    @Override
    protected String internalTranslateWeekStart(Expression expr, String content){
        return String.format(SQL_START_WEEK, content);
    }

    @Override
    protected String internalTranslateWeekEnd(Expression expr, String content){
        return String.format(SQL_END_WEEK, content);
    }

    @Override
    protected String internalTranslateQuartStart(Expression expr, String content) {
        return String.format(SQL_START_QUART, content);
    }

    @Override
    protected String internalTranslateQuartEnd(Expression expr, String content) {
        return String.format(SQL_END_QUART, content);
    }

    @Override
    protected String internalTranslateNextDay(Expression expr, String content){
        return String.format(SQL_NEXT_DAY, content);
    }

    protected String internalOffset(String templateSql, String content) {
        String[] args = StringUtils.split(content, ExprConsts.ARG_DELIMITER);
        Preconditions.checkState(args.length > 1);
        return String.format(templateSql, args[0], args[1]);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetYears(Expression expr, String content){
        return internalOffset(SQL_OFFSET_YEARS, content);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetMonths(Expression expr, String content){
        return internalOffset(SQL_OFFSET_MONTHS, content);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetDays(Expression expr, String content){
        return internalOffset(SQL_OFFSET_DAYS, content);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetQuarts(Expression expr, String content){
        return internalOffset(SQL_OFFSET_QUARTS, content);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetWeeks(Expression expr, String content){
        return internalOffset(SQL_OFFSET_WEEKS, content);
    }
}
