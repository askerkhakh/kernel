package ru.sonarplus.kernel.sqlobject.expressions;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ru.sonarplus.kernel.sqlobject.expressions.ExpressionTreeBuilder.ArgRefs.*;
import static ru.sonarplus.kernel.sqlobject.expressions.ExpressionTreeBuilder.SymbolType.*;

public class ExpressionTreeBuilder {

    protected enum SymbolType {
        CHARS, PARAM, NAMED_REF, UNNAMED_REF, EXPR_BEGIN, EXPR_END
    }

    public enum ArgRefs {
        NOTHING, UNNAMED, NAMED
    }

    protected static class ExtractedSymbol {
        public String symbol;
        public SymbolType symbolType;

        public ExtractedSymbol() {}

        public static ExtractedSymbol getES(SymbolType symbolType, String symbol, ExtractedSymbol earlyExtractedSymbol) {
            ExtractedSymbol result = earlyExtractedSymbol;
            if (result == null)
                result = new ExtractedSymbol();
            result.symbolType = symbolType;
            result.symbol = symbol;
            return result;
        }
    }

    protected static ExtractedSymbol extractSymbol(String str, int fromPos, ExtractedSymbol earlyExtractedSymbol) {
        if (str.startsWith(ExprConsts.EXPR_BEGIN, fromPos))
            return ExtractedSymbol.getES(EXPR_BEGIN, ExprConsts.EXPR_BEGIN, earlyExtractedSymbol);
        else if (str.startsWith(ExprConsts.EXPR_END, fromPos))
            return ExtractedSymbol.getES(EXPR_END, ExprConsts.EXPR_END, earlyExtractedSymbol);
        else {
            String symbol;
            switch (str.charAt(fromPos)) {
                case '\'':
                    return ExtractedSymbol.getES(CHARS, ExprUtils.getLiteral(str, fromPos), earlyExtractedSymbol);

                case Expression.CHAR_BEFORE_TOKEN:
                    symbol = ExprUtils.getToken(str, fromPos);
                    if (symbol.equals(Expression.UNNAMED_ARGUMENT_REF))
                        return ExtractedSymbol.getES(UNNAMED_REF, symbol, earlyExtractedSymbol);
                    else
                        return ExtractedSymbol.getES(NAMED_REF, symbol, earlyExtractedSymbol);

                case Expression.CHAR_BEFORE_PARAMETER:
                    symbol = ExprUtils.getParamName(str, fromPos);
                    return ExtractedSymbol.getES(PARAM, symbol, earlyExtractedSymbol);

                default:
                    return ExtractedSymbol.getES(CHARS, Character.toString(str.charAt(fromPos)), earlyExtractedSymbol);
            }
        }
    }

    protected static void optimizeExprNode(Expression expr)
            throws SqlObjectException {
        String sourceExpr = expr.getExpr();
        if (sourceExpr.equals(Expression.UNNAMED_ARGUMENT_REF)) {
            expr.getOwner().replace(expr, expr.firstSubItem());
        }
        else {
            sourceExpr = sourceExpr
                    .substring(0, sourceExpr.length() - ExprConsts.EXPR_END.length())
                    .substring(ExprConsts.EXPR_BEGIN.length());
            if (sourceExpr.regionMatches(true, 0, ExprConsts.QNAME, 0, ExprConsts.QNAME.length())) {
                sourceExpr = sourceExpr.substring(ExprConsts.QNAME.length() + ExprConsts.DELIMITER.length());
                QualifiedName qname = QualifiedName.stringToQualifiedName(sourceExpr);
                expr.getOwner().replace(expr, new QualifiedField(null, qname.alias, qname.name));
            }
            else if (sourceExpr.regionMatches(true, 0, ExprConsts.QRNAME, 0, ExprConsts.QRNAME.length())) {
                sourceExpr = sourceExpr.substring(ExprConsts.QRNAME.length() + ExprConsts.DELIMITER.length());
                QualifiedName qname = QualifiedName.stringToQualifiedName(sourceExpr);
                expr.getOwner().replace(expr, new QualifiedRField(null, qname.alias, qname.name));
            }
            else if (sourceExpr.regionMatches(true, 0, ExprConsts.FUNCTION_BINDATA_TOTEXT, 0, ExprConsts.FUNCTION_BINDATA_TOTEXT.length())) {
                //
            }
            else {
                //
            }
        }
    }

    protected static ColumnExpression getResult(Expression expr)
            throws SqlObjectException {
        if (expr.getExpr().equals(Expression.UNNAMED_ARGUMENT_REF)) {
            ColumnExpression result = (ColumnExpression)expr.firstSubItem();
            expr.removeItem(result);
            return result;
        }
        else
            return expr;
    }

    protected static Expression startInternalExpression(SqlObject owner, Map<Expression, StringBuilder> expressionStringBuilders)
            throws SqlObjectException {
        Expression result = new Expression(owner, "", false);
        expressionStringBuilders.put(result, new StringBuilder().append(ExprConsts.EXPR_BEGIN));
        return result;
    }

    protected static void finallyInternalExpression(Expression expression, Map<Expression, StringBuilder> expressionStringBuilders) {
        StringBuilder builder = expressionStringBuilders.get(expression);
        builder.append(ExprConsts.EXPR_END);
        expression.setExpr(builder.toString());
        expressionStringBuilders.remove(expression);
    }

    protected static ColumnExpression internalExecute(Expression source) {
        Set<SqlObject> usedArguments = new HashSet<>();
        Map<Expression, StringBuilder> expressionStringBuilders = new HashMap<>();
        int unnamedRefIndex = 0;
        ArgRefs argRefs = NOTHING;

        String strExpr = source.getExpr();
        if (StringUtils.isEmpty(strExpr))
            throw new ExpressionException(String.format("Объект '%s' не содержит строку выражения", source.getClass().getSimpleName()));

        Expression newExpr = new Expression(null, "", false);
        expressionStringBuilders.put(newExpr, new StringBuilder());

        Expression currentExpr = newExpr;
        SqlObject child;
        int currentPos = 0;
        ExtractedSymbol symbol = null;
        while (currentPos < strExpr.length()) {
            symbol = extractSymbol(strExpr, currentPos, symbol);
            switch (symbol.symbolType) {
                case CHARS:
                    expressionStringBuilders.get(currentExpr).append(symbol.symbol);
                    break;

                case PARAM:
                    symbol.symbol = symbol.symbol.substring(1);
                    child = source.findById(symbol.symbol);
                    if (argRefs != UNNAMED && child != null) {
                        // если извлечённые до этого ссылки не являлись безымянными (позиционными)
                        // и есть подчинённый элемент с Id == symbol.symbol
                        // и этот элемент является параметром - считаем, что далее ссылки именованые
                        argRefs = NAMED;
                        Preconditions.checkArgument(child instanceof Parameter);
                        currentExpr.insertItem(child.getClone());
                        usedArguments.add(child);
                    }
                    else {
                        // в противном случае считаем, что ссылка безымянная (позиционная) и далее
                        // в исходном выражении будут такие же.
                        argRefs = UNNAMED;
                        new ParamRef(currentExpr, symbol.symbol);
                    }
                    expressionStringBuilders.get(currentExpr).append(Expression.UNNAMED_ARGUMENT_REF);
                    break;

                case NAMED_REF:
                    if (!(argRefs == NOTHING || argRefs == NAMED))
                        throw new ExpressionException(String.format(ExprUtils.MIXED_NAMED_AND_UNNAMED_REFS, source.getExpr()));
                    argRefs = NAMED;
                    child = source.findById(symbol.symbol.substring(1));
                    if (child == null)
                        throw new ExpressionException(String.format(ExprUtils.NOT_FOUND_ARGUMENT_OF_EXPRESSION_BY_REF, source.getExpr(), symbol));
                    currentExpr.insertItem(child.getClone());
                    usedArguments.add(child);
                    expressionStringBuilders.get(currentExpr).append(Expression.UNNAMED_ARGUMENT_REF);
                    break;

                case UNNAMED_REF:
                    if (!(argRefs == NOTHING || argRefs == UNNAMED))
                        throw new ExpressionException(String.format(ExprUtils.MIXED_NAMED_AND_UNNAMED_REFS, source.getExpr()));
                    if (!(unnamedRefIndex < source.itemsCount()))
                        throw new ExpressionException(String.format(ExprUtils.UNNAMED_REFS_COUNT_MORE_THAN_ARGUMENTS_COUNT, source.getExpr()));
                    argRefs = UNNAMED;
                    child = source.getItem(unnamedRefIndex);
                    currentExpr.insertItem(child.getClone());
                    usedArguments.add(child);
                    unnamedRefIndex ++;
                    expressionStringBuilders.get(currentExpr).append(Expression.UNNAMED_ARGUMENT_REF);
                    break;

                case EXPR_BEGIN:
                    expressionStringBuilders.get(currentExpr).append(Expression.UNNAMED_ARGUMENT_REF);
                    currentExpr = startInternalExpression(currentExpr, expressionStringBuilders);
                    break;

                case EXPR_END:
                    if (currentExpr != newExpr)
                        finallyInternalExpression(currentExpr, expressionStringBuilders);
                    Expression subExpr = currentExpr;
                    currentExpr = (Expression) subExpr.getOwner();
                    Preconditions.checkNotNull(currentExpr); // количество закрывающих тегов sExprEnd больше количества открывающих sExprBegin
                    optimizeExprNode(subExpr);
                    break;

                default:
                    Preconditions.checkArgument(false);
            }
            currentPos += symbol.symbol.length();
        }
        // убедимся, что количество закрывающих тегов ExprEnd соответствует количеству открывающих ExprBegin
        Preconditions.checkArgument(currentExpr == newExpr);
        // убедимся, что все подчинённые выражению аргументы былы соотнесены со ссылками на них из выражения
        for (SqlObject item: source)
            if (!usedArguments.contains(item))
                throw new ExpressionException(String.format(ExprUtils.EXPR_HAS_ARGUMENTS_THAT_NOT_CORRESPONDS_WITH_REFS, source.getExpr()));


        newExpr.setExpr(expressionStringBuilders.get(currentExpr).toString());
        return getResult(newExpr);
    }

    public  static ColumnExpression execute(Expression source) {
        ColumnExpression result = internalExecute(source);
        if (source.distTechInfo == null) {
            source.distTechInfo = new ColumnExprTechInfo();
        }
        result.distTechInfo = source.distTechInfo.getClone();
        return result;
    }
}
