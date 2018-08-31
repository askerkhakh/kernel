package ru.sonarplus.kernel.sqlobject.db_support_ora;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDBRelativeDateSupport;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.objects.Expression;

public class SqlObjectsDBRelativeDateSupportOra extends SqlObjectsDBRelativeDateSupport {
    @Override
    protected String internalTranslateToday() {
        return "TRUNC(SYSDATE)";
    }

    @Override
    protected String internalTranslateCurrentTime() {
        return "TO_DATE('18720101 ' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')";
    }

    @Override
    protected String internalTranslateYearStart(Expression expr, String content) {
        return String.format("TRUNC(%s,'YY')", content);
    }

    @Override
    protected String internalTranslateYearEnd(Expression expr, String content) {
        return String.format("ADD_MONTHS(TRUNC(%s,'YY'),12)-1", content);
    }

    @Override
    protected String internalTranslateMonthStart(Expression expr, String content) {
        return String.format("TRUNC(%s,'MM')", content);
    }

    @Override
    protected String internalTranslateMonthEnd(Expression expr, String content) {
        return String.format("LAST_DAY(%s)", content);
    }

    @Override
    protected String internalTranslateWeekStart(Expression expr, String content) {
        // зависит от параметра NLS_TERRITORY
        return String.format("TRUNC(%s, 'DY')", content);
    }

    @Override
    protected String internalTranslateWeekEnd(Expression expr, String content) {
        // зависит от параметра NLS_TERRITORY
        return String.format("TRUNC(%s, 'DY') + 6", content);
    }

    @Override
    protected String internalTranslateQuartStart(Expression expr, String content) {
        return String.format("TRUNC(%s,'Q')", content);
    }

    @Override
    protected String internalTranslateQuartEnd(Expression expr, String content) {
        //return String.format("TRUNC(ADD_MONTHS(%s, 3),'Q')-1", content);
        return String.format("ADD_MONTHS(TRUNC(%s,'Q'), 3)-1", content);
    }

    @Override
    protected String internalTranslateNextDay(Expression expr, String content) {
        return content + "+1";
    }

    @Override
    protected String internalTranslateRelativeDateOffsetYears(Expression expr, String content) {
        String[] contentItems = getContentItems(content);
        return String.format("ADD_MONTHS(%s,(%s)*12)", contentItems[0], contentItems[1]);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetMonths(Expression expr, String content) {
        String[] contentItems = getContentItems(content);
        return String.format("ADD_MONTHS(%s,(%s))", contentItems[0], contentItems[1]);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetDays(Expression expr, String content) {
        String[] contentItems = getContentItems(content);
        return String.format("%s+(%s)", contentItems[0], contentItems[1]);
    }

    @Override
    protected String internalTranslateRelativeDateOffsetQuarts(Expression expr, String content) {
        String[] contentItems = getContentItems(content);
        return String.format("ADD_MONTHS(%s,(%s)*3)", contentItems[0], contentItems[1]);
    }

    private String[] getContentItems(String content) {
        String[] contentItems = content.split(ExprConsts.ARG_DELIMITER);
        Preconditions.checkArgument(contentItems.length > 1);
        return contentItems;
    }

    @Override
    protected String internalTranslateRelativeDateOffsetWeeks(Expression expr, String content) {
        String[] contentItems = getContentItems(content);
        return String.format("%s+(%s)*7", contentItems[0], contentItems[1]);
    }
}
