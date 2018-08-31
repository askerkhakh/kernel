package ru.sonarplus.kernel.sqlobject.expressions;

import com.google.common.base.Preconditions;

import java.util.HashMap;

public class ExprRelativeDate {

    public final static String RELDATE_TODAY = "expr_reldate_today";
    public final static String RELDATE_CURRENT_TIME = "expr_reldate_current_time";
    public final static String RELDATE_YEAR_START = "expr_reldate_year_start";
    public final static String RELDATE_YEAR_END = "expr_reldate_year_end";
    public final static String RELDATE_MONTH_START = "expr_reldate_month_start";
    public final static String RELDATE_MONTH_END = "expr_reldate_month_end";
    public final static String RELDATE_WEEK_START = "expr_reldate_week_start";
    public final static String RELDATE_WEEK_END = "expr_reldate_week_end";
    public final static String RELDATE_QUART_START = "expr_reldate_quart_start";
    public final static String RELDATE_QUART_END = "expr_reldate_quart_end";
    public final static String RELDATE_NEXT_DAY = "expr_reldate_next_day";

    public final static String RELDATE_OFFSET_YEARS = "expr_reldate_relative_offset_years";
    public final static String RELDATE_OFFSET_MONTHS = "expr_reldate_relative_offset_months";
    public final static String RELDATE_OFFSET_DAYS = "expr_reldate_relative_offset_days";
    public final static String RELDATE_OFFSET_QUARTS = "expr_reldate_relative_offset_quarts";
    public final static String RELDATE_OFFSET_WEEKS = "expr_reldate_relative_offset_weeks";

    public enum RelativeDateValueScale {YEAR, MONTH, DAY, FOUR_MONTHS, WEEK}

    protected static String exprRelDateCommon(String tag, String content) {
        return ExprConsts.EXPR_BEGIN + tag + ExprConsts.DELIMITER + content + ExprConsts.EXPR_END;
    }

    public static String exprToday() {
        return exprRelDateCommon(RELDATE_TODAY, "");
    }

    public static String exprCurrentTime() {
        return exprRelDateCommon(RELDATE_CURRENT_TIME, "");
    }

    public static String exprYearStart(String exprDate) {
        return exprRelDateCommon(RELDATE_YEAR_START, exprDate);
    }

    public static String exprYearEnd(String exprDate) {
        return exprRelDateCommon(RELDATE_YEAR_END, exprDate);
    }

    public static String exprMonthStart(String exprDate) {
        return exprRelDateCommon(RELDATE_MONTH_START, exprDate);
    }

    public static String exprMonthEnd(String exprDate) {
        return exprRelDateCommon(RELDATE_MONTH_END, exprDate);
    }

    public static String exprWeekStart(String exprDate) {
        return exprRelDateCommon(RELDATE_WEEK_START, exprDate);
    }

    public static String exprWeekEnd(String exprDate) {
        return exprRelDateCommon(RELDATE_WEEK_END, exprDate);
    }

    public static String exprQuartStart(String exprDate) {
        return exprRelDateCommon(RELDATE_QUART_START, exprDate);
    }

    public static String exprQuartEnd(String exprDate) {
        return exprRelDateCommon(RELDATE_QUART_END, exprDate);
    }

    public static String exprDateModifier(String exprDate,
        RelativeDateValueScale baseModifier,  boolean baseModifierBegin) {
        if (baseModifierBegin)
            switch(baseModifier) {
                case YEAR:
                    return exprYearStart(exprDate);

                case MONTH:
                    return exprMonthStart(exprDate);

                case DAY:
                    return exprDate;

                case FOUR_MONTHS:
                    return exprQuartStart(exprDate);

                case WEEK:
                    return exprWeekStart(exprDate);

                default:
                    Preconditions.checkArgument(false);
                    return null;
            }
        else
            switch(baseModifier) {
                case YEAR:
                    return exprYearEnd(exprDate);

                case MONTH:
                    return exprMonthEnd(exprDate);

                case DAY:
                    return exprNextDay((exprDate));

                case FOUR_MONTHS:
                    return exprQuartEnd(exprDate);

                case WEEK:
                    return exprWeekEnd(exprDate);

                default:
                    Preconditions.checkArgument(false);
                    return null;
            }
    }

    public static String exprNextDay(String exprDate) {
        return exprRelDateCommon(RELDATE_NEXT_DAY, exprDate);
    }

    protected static final HashMap<RelativeDateValueScale, String> relativeDateTags = new HashMap<RelativeDateValueScale, String>(){
        {
            put(RelativeDateValueScale.YEAR, RELDATE_OFFSET_YEARS);
            put(RelativeDateValueScale.MONTH, RELDATE_OFFSET_MONTHS);
            put(RelativeDateValueScale.DAY, RELDATE_OFFSET_DAYS);
            put(RelativeDateValueScale.FOUR_MONTHS, RELDATE_OFFSET_QUARTS);
            put(RelativeDateValueScale.WEEK, RELDATE_OFFSET_WEEKS);
        }
    };

    public static String exprRelativeDate(String exprDate, String exprOffset, RelativeDateValueScale offsetScale) {
        return exprRelDateCommon(
                relativeDateTags.get(offsetScale),
                String.join(ExprConsts.ARG_DELIMITER, exprDate, exprOffset));
    }
}
