package ru.sonarplus.kernel.sqlobject.db_support;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.expressions.ExprRelativeDate;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static ru.sonarplus.kernel.sqlobject.objects.QueryParam.ParamType.INPUT;

public abstract class SqlObjectsDBRelativeDateSupport {

    public static final String PARAM_TODAY = "P_TODAY_BQ593EQE9";
    public static final String PARAM_CURRENT_TIME = "P_CURRTIME_BQ593EQE9";

    private class RelDateTranslationContext {
        public Expression expr;
        public String exprContent;
        public RelDateTranslationContext(Expression expr, String exprContent) {
            this.expr = expr;
            this.exprContent = exprContent;
        }
    }

    @FunctionalInterface
    public interface FunctionRelDateTranslator {
        String apply(RelDateTranslationContext t);
    }

    private Map<String, FunctionRelDateTranslator> translatorsMap = new HashMap<>();

    public SqlObjectsDBRelativeDateSupport() {
        translatorsMap.put(ExprRelativeDate.RELDATE_YEAR_START, this::translateYearStart);
        translatorsMap.put(ExprRelativeDate.RELDATE_YEAR_END, this::translateYearEnd);
        translatorsMap.put(ExprRelativeDate.RELDATE_MONTH_START, this::translateMonthStart);
        translatorsMap.put(ExprRelativeDate.RELDATE_MONTH_END, this::translateMonthEnd);
        translatorsMap.put(ExprRelativeDate.RELDATE_WEEK_START, this::translateWeekStart);
        translatorsMap.put(ExprRelativeDate.RELDATE_WEEK_END, this::translateWeekEnd);
        translatorsMap.put(ExprRelativeDate.RELDATE_QUART_START, this::translateQuartStart);
        translatorsMap.put(ExprRelativeDate.RELDATE_QUART_END, this::translateQuartEnd);
        translatorsMap.put(ExprRelativeDate.RELDATE_NEXT_DAY, this::translateNextDay);

        translatorsMap.put(ExprRelativeDate.RELDATE_OFFSET_YEARS, this::translateRelativeDateOffsetYears);
        translatorsMap.put(ExprRelativeDate.RELDATE_OFFSET_MONTHS, this::translateRelativeDateOffsetMonths);
        translatorsMap.put(ExprRelativeDate.RELDATE_OFFSET_DAYS, this::translateRelativeDateOffsetDays);
        translatorsMap.put(ExprRelativeDate.RELDATE_OFFSET_QUARTS, this::translateRelativeDateOffsetQuarts);
        translatorsMap.put(ExprRelativeDate.RELDATE_OFFSET_WEEKS, this::translateRelativeDateOffsetWeeks);
    }

    protected abstract String internalTranslateToday();
    protected abstract String internalTranslateCurrentTime();
    protected abstract String internalTranslateYearStart(Expression expr, String content);
    protected abstract String internalTranslateYearEnd(Expression expr, String content);
    protected abstract String internalTranslateMonthStart(Expression expr, String content);
    protected abstract String internalTranslateMonthEnd(Expression expr, String content);
    protected abstract String internalTranslateWeekStart(Expression expr, String content);
    protected abstract String internalTranslateWeekEnd(Expression expr, String content);
    protected abstract String internalTranslateQuartStart(Expression expr, String content);
    protected abstract String internalTranslateQuartEnd(Expression expr, String content);
    protected abstract String internalTranslateNextDay(Expression expr, String content);
    protected abstract String internalTranslateRelativeDateOffsetYears(Expression expr, String content);
    protected abstract String internalTranslateRelativeDateOffsetMonths(Expression expr, String content);
    protected abstract String internalTranslateRelativeDateOffsetDays(Expression expr, String content);
    protected abstract String internalTranslateRelativeDateOffsetQuarts(Expression expr, String content);
    protected abstract String internalTranslateRelativeDateOffsetWeeks(Expression expr, String content);

    /*
    При трансляции выражений с относительными датами сам объект-выражение в большинстве случаев
    не был бы нужен, т.е. достаточно было бы сформировать строку, за исключением следующих случаев:
    1. использование "локальных" даты/времени (сервера приложения) - в этом случае формируется
       строка не с вызовом соответствующей функции БД, а сот ссылкой на аргумент '??',
       и к выражению "цепляется" ссылка (ParamRef) на параметр ()ParamStatic со значением локальной даты/времени
    2. в Delphi, при перекрытии трансляции функций получения начала/окончания квартала для Sqlite
       (TSqlObjectsDBSupport_RelativeDate_SQLite.DoTranslate_QuartStart/End) пришлось дублировать
       аргумент выражения.
    т.е. в этих двух случаях нужен доступ к аргументам выражения,
    а в случае 1 - к разделу параметров запроса.
    */
    protected  String translateCurrentTime(SqlQuery root, Expression expr, LocalDateTime fixedCurrentDateTime)
            throws SqlObjectException {
        if (fixedCurrentDateTime == null) {
            // вызов функции БД
            return internalTranslateCurrentTime();
        }
        else {
            // выражение, параметризованое текущей датой
            QueryParams params = root.newParams();
            if (params.findParam(PARAM_CURRENT_TIME) == null)
                new QueryParam(params, PARAM_CURRENT_TIME, new ValueConst(fixedCurrentDateTime, FieldTypeId.tid_TIME), INPUT);
            new ParamRef(expr, PARAM_CURRENT_TIME);
            return Expression.UNNAMED_ARGUMENT_REF;
        }
    }

    protected  String translateToday(SqlQuery root, Expression expr, LocalDateTime fixedCurrentDateTime)
            throws SqlObjectException {
        if (fixedCurrentDateTime == null) {
            // вызов функции БД
            return internalTranslateToday();
        }
        else {
            // выражение, параметризованое текущим временем
            QueryParams params = root.newParams();
            if (params.findParam(PARAM_TODAY) == null)
                new QueryParam(params, PARAM_TODAY, new ValueConst(fixedCurrentDateTime, FieldTypeId.tid_DATE), INPUT);
            new ParamRef(expr, PARAM_TODAY);
            return Expression.UNNAMED_ARGUMENT_REF;
        }
    }

    protected String translateYearStart(RelDateTranslationContext context) {
        return internalTranslateYearStart(context.expr, context.exprContent);
    }
    protected String translateYearEnd(RelDateTranslationContext context) {
        return internalTranslateYearEnd(context.expr, context.exprContent);
    }
    protected String translateMonthStart(RelDateTranslationContext context) {
        return internalTranslateMonthStart(context.expr, context.exprContent);
    }
    protected String translateMonthEnd(RelDateTranslationContext context) {
        return internalTranslateMonthEnd(context.expr, context.exprContent);
    }
    protected String translateWeekStart(RelDateTranslationContext context) {
        return internalTranslateWeekStart(context.expr, context.exprContent);
    }
    protected String translateWeekEnd(RelDateTranslationContext context) {
        return internalTranslateWeekEnd(context.expr, context.exprContent);
    }
    protected String translateQuartStart(RelDateTranslationContext context) {
        return internalTranslateQuartStart(context.expr, context.exprContent);
    }
    protected String translateQuartEnd(RelDateTranslationContext context) {
        return internalTranslateQuartEnd(context.expr, context.exprContent);
    }
    protected String translateNextDay(RelDateTranslationContext context) {
        return internalTranslateNextDay(context.expr, context.exprContent);
    }
    protected String translateRelativeDateOffsetYears(RelDateTranslationContext context) {
        return internalTranslateRelativeDateOffsetYears(context.expr, context.exprContent);
    }
    protected String translateRelativeDateOffsetMonths(RelDateTranslationContext context) {
        return internalTranslateRelativeDateOffsetMonths(context.expr, context.exprContent);
    }
    protected String translateRelativeDateOffsetDays(RelDateTranslationContext context) {
        return internalTranslateRelativeDateOffsetDays(context.expr, context.exprContent);
    }
    protected String translateRelativeDateOffsetQuarts(RelDateTranslationContext context) {
        return internalTranslateRelativeDateOffsetQuarts(context.expr, context.exprContent);
    }
    protected String translateRelativeDateOffsetWeeks(RelDateTranslationContext context) {
        return internalTranslateRelativeDateOffsetWeeks(context.expr, context.exprContent);
    }

    public boolean tryTranslate(SqlQuery root, Expression expr, String tag, String content, LocalDateTime fixedCurrentDateTime) {
        boolean result = true;
        if (tag.equals(ExprRelativeDate.RELDATE_TODAY))
            expr.setExpr(translateToday(root, expr, fixedCurrentDateTime));
        else if (tag.equals(ExprRelativeDate.RELDATE_CURRENT_TIME))
            expr.setExpr(translateCurrentTime(root, expr, fixedCurrentDateTime));
        else {
            FunctionRelDateTranslator func = translatorsMap.get(tag);
            result = func != null;
            if (result)
                expr.setExpr(func.apply(new RelDateTranslationContext(expr, content)));
        }
        return result;
    }
}
