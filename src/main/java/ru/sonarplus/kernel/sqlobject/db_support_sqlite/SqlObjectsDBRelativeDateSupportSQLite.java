package ru.sonarplus.kernel.sqlobject.db_support_sqlite;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDBRelativeDateSupport;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.objects.Expression;

public class SqlObjectsDBRelativeDateSupportSQLite extends SqlObjectsDBRelativeDateSupport {

    public SqlObjectsDBRelativeDateSupportSQLite() { super(); }

    static final String SQL_START_YEAR = "/*<start of year>*/ DATE(DATE(%s, 'UNIXEPOCH'), 'START OF YEAR') /*</start of year>*/";
    static final String SQL_END_YEAR = "/*<end of year>*/ DATE(DATE(%s, 'UNIXEPOCH'), 'START OF YEAR', '+12 MONTHS', '-1 DAYS') /*</end of year>*/";
    static final String SQL_START_MONTH = "/*<start of month>*/ DATE(DATE(%s, 'UNIXEPOCH'), 'START OF MONTH') /*</start of month>*/";
    static final String SQL_END_MONTH = "/*<end of month>*/DATE(DATE(%s, 'UNIXEPOCH'), 'START OF MONTH', '+1 MONTHS', '-1 DAYS') /*</end of month>*/";
    static final String SQL_START_WEEK = "/*<start of week>*/ DATE(DATE(DATE(%s, 'UNIXEPOCH'), 'WEEKDAY 6'), '-5 DAYS') /*</start of week>*/";
    static final String SQL_END_WEEK = "/*<end of week>*/ DATE(DATE(DATE(%s, 'UNIXEPOCH'), 'WEEKDAY 6'), '+1 DAYS') /*</end of week>*/";

    static final String SQL_START_QUART =
                "/*<start of quart>*/" +
                        "DATE(DATE(%s, 'UNIXEPOCH'),'START OF YEAR',CAST(((STRFTIME('%%m', DATE(%s, 'UNIXEPOCH'))+2)/3-1)*3 AS TEXT) || ' MONTHS')" +
                "/*</start of quart>*/";

    static final String SQL_END_QUART =
                "/*<end of quart>*/" +
                        "DATE(DATE(%s, 'UNIXEPOCH'), 'START OF YEAR', CAST((STRFTIME('%%m', DATE(%s, 'UNIXEPOCH'))+2)/3*3 AS TEXT) || ' MONTHS', '-1 DAYS')" +
                "/*</end of quart>*/";

    static final String SQL_NEXT_DAY = "DATE(DATE(%s, 'UNIXEPOCH'), '+1 days')";

    static final String SQL_OFFSET_YEARS = "DATE(DATE(%s, 'UNIXEPOCH'), CAST(%s AS TEXT) || ' YEARS')";
    static final String SQL_OFFSET_MONTHS = "DATE(DATE(%s, 'UNIXEPOCH'), CAST(%s AS TEXT) || ' MONTHS')";
    static final String SQL_OFFSET_DAYS = "DATE(DATE(%s, 'UNIXEPOCH'), CAST(%s AS TEXT) || ' DAYS')";
    static final String SQL_OFFSET_QUARTS = "DATE(DATE(%s, 'UNIXEPOCH'), CAST((%s * 3) AS TEXT) || ' MONTHS')";
    static final String SQL_OFFSET_WEEKS = "DATE(DATE(%s, 'UNIXEPOCH'), CAST((%s * 7) AS TEXT) || ' DAYS')";

    @Override
    protected String internalTranslateToday(){
        // strftime('%s', 'now') вернёт unixtime для текущего момента (дата+время)
        //    date('now') отбрасывает время
        return "STRFTIME('%s', DATE('NOW'))";
    }

    @Override
    protected String internalTranslateCurrentTime(){
        // "отбросим" дату - заменим её на 01.01.1872
        return "DATETIME(STRFTIME('1872-01-01 %H:%M:%S', DATETIME('NOW')))";
    }

    @Override
    protected String internalTranslateYearStart(Expression expr, String content){
        // полагаем, что в AContent содержится Unixtime-выражение
        return "STRFTIME('%s', " + String.format(SQL_START_YEAR, content) + ')';
    }

    @Override
    protected String internalTranslateYearEnd(Expression expr, String content){
        return "STRFTIME('%s', " + String.format(SQL_END_YEAR, content) + ')';
    }

    @Override
    protected String internalTranslateMonthStart(Expression expr, String content){
        return "STRFTIME('%s', " + String.format(SQL_START_MONTH, content) + ")";
    }

    @Override
    protected String internalTranslateMonthEnd(Expression expr, String content){
        return "STRFTIME('%s', " + String.format(SQL_END_MONTH, content) + ")";
    }

    @Override
    protected String internalTranslateWeekStart(Expression expr, String content){
        return "STRFTIME('%s', " + String.format(SQL_START_WEEK, content) + ")";
    }

    @Override
    protected String internalTranslateWeekEnd(Expression expr, String content){
        return "STRFTIME('%s', " + String.format(SQL_END_WEEK, content) + ")";
    }

    @Override
    protected String internalTranslateQuartStart(Expression expr, String content) {
        // TODO в Delphi была реализована callback-функция, Date_StartOfQuartal, заменяющая всю эту конструкцию. Может быть и здесь также?
        Preconditions.checkArgument(expr.itemsCount() == 1);
        // т.к. исходная дата в выражении присутствует два раза (дата раз/дата два) - скопируем её
        expr.insertItem(expr.firstSubItem().getClone());
        return "STRFTIME('%s', " + String.format(SQL_START_QUART, content, content) + ")";
    }

    @Override
    protected String internalTranslateQuartEnd(Expression expr, String content) {
        Preconditions.checkArgument(expr.itemsCount() == 1);
        // т.к. исходная дата в выражении присутствует два раза (дата раз/дата два) - скопируем её
        expr.insertItem(expr.firstSubItem().getClone());
        return "STRFTIME('%s', " + String.format(SQL_END_QUART, content, content) + ")";
    }

    @Override
    protected String internalTranslateNextDay(Expression expr, String content){
        return "STRFTIME('%s', " + String.format(SQL_NEXT_DAY, content, content) + ")";
    }

    @Override
    protected String internalTranslateRelativeDateOffsetYears(Expression expr, String content){
        String[] args = StringUtils.split(content, ExprConsts.ARG_DELIMITER);
        Preconditions.checkState(args.length > 1);
        return "STRFTIME('%s', " + String.format(SQL_OFFSET_YEARS, args[0], args[1]) + ")";
    }

    @Override
    protected String internalTranslateRelativeDateOffsetMonths(Expression expr, String content){
        String[] args = StringUtils.split(content, ExprConsts.ARG_DELIMITER);
        Preconditions.checkState(args.length > 1);
        return "STRFTIME('%s', " + String.format(SQL_OFFSET_MONTHS, args[0], args[1]) + ")";
    }

    @Override
    protected String internalTranslateRelativeDateOffsetDays(Expression expr, String content){
        String[] args = StringUtils.split(content, ExprConsts.ARG_DELIMITER);
        Preconditions.checkState(args.length > 1);
        return "STRFTIME('%s', " + String.format(SQL_OFFSET_DAYS, args[0], args[1]) + ")";
    }

    @Override
    protected String internalTranslateRelativeDateOffsetQuarts(Expression expr, String content){
        String[] args = StringUtils.split(content, ExprConsts.ARG_DELIMITER);
        Preconditions.checkState(args.length > 1);
        return "STRFTIME('%s', " + String.format(SQL_OFFSET_QUARTS, args[0], args[1]) + ")";
    }

    @Override
    protected String internalTranslateRelativeDateOffsetWeeks(Expression expr, String content){
        String[] args = StringUtils.split(content, ExprConsts.ARG_DELIMITER);
        Preconditions.checkState(args.length > 1);
        return "STRFTIME('%s', " + String.format(SQL_OFFSET_WEEKS, args[0], args[1]) + ")";
    }
}
