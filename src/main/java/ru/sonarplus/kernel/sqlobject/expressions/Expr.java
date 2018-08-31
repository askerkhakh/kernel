package ru.sonarplus.kernel.sqlobject.expressions;

import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;

import java.util.ArrayList;
import java.util.List;

public class Expr {

    public Expr() {
    }

    protected static String exprQNameCommon(String tag, String alias, String name) {
        return ExprConsts.EXPR_BEGIN + tag + ExprConsts.DELIMITER +
                QualifiedName.formQualifiedNameString(alias, name) + ExprConsts.EXPR_END;
    }

    protected static String exprFunctionCommon(String tag, List<String> exprs) {
        return ExprConsts.EXPR_BEGIN + tag + ExprConsts.DELIMITER + String.join(",", exprs) + ExprConsts.EXPR_END;
    }

    protected static String exprFunctionCommon(String tag, String[] exprs) {
        ArrayList<String> listExprs = new ArrayList<String>();
        for (String expr : exprs)
            listExprs.add(expr);
    return exprFunctionCommon(tag, listExprs);
    }
 
    public static String exprQRName(String name) {
        return exprQRName("", name);
    }

    public static String exprQRName(String alias, String name) {
        return exprQNameCommon(ExprConsts.QRNAME, alias, name);
    }

    public static String exprQName(String name) {
        return exprQName("", name);
    }

    public static String exprQName(String alias, String name) {
        return exprQNameCommon(ExprConsts.QNAME, alias, name);
    }

    public static String exprBinDataToText(String alias, String name) { return exprQNameCommon(ExprConsts.FUNCTION_BINDATA_TOTEXT, alias, name); }

    public static String exprBinDataToText(String name) { return exprBinDataToText("", name); }

    public static String exprUpper(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_UPPER, new String[]{argument});
    }

    public static String exprCount(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_COUNT, new String[]{argument});
    }

    public static String exprMax(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_MAX, new String[]{argument});
    }

    public static String exprMin(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_MIN, new String[]{argument});
    }

    public static String exprSum(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_SUM, new String[]{argument});
    }

    public static String exprAvg(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_AVG, new String[]{argument});
    }

    public static String exprCoalesce(String[] arguments) {
        return exprFunctionCommon(ExprConsts.FUNCTION_COALESCE, arguments);
    }

    public static String exprCoalesce(List<String> arguments) {
        return exprFunctionCommon(ExprConsts.FUNCTION_COALESCE, arguments);
    }

    public static String exprRound(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_ROUND, new String[]{argument});
    }

    public static String exprYearFromDate(String argument) {
        return exprFunctionCommon(ExprConsts.FUNCTION_YEAR_FROM_DATE, new String[]{argument});
    }
}
