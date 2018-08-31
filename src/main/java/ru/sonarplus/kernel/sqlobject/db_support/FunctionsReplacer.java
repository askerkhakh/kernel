package ru.sonarplus.kernel.sqlobject.db_support;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.objects.Expression;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;

import java.time.LocalDateTime;

public class FunctionsReplacer {

    protected static class ExtractedExprParts{
        public String partOfTag, partOfContent;
        public ExtractedExprParts() {
            this.partOfTag = "";
            this.partOfContent = "";
        }
    }

    protected static ExtractedExprParts extractExprParts(String expr) {
        ExtractedExprParts result = null;
        if (expr.startsWith(ExprConsts.EXPR_BEGIN)) {
            Preconditions.checkArgument(expr.endsWith(ExprConsts.EXPR_END));
            String midExpr = expr.substring(ExprConsts.EXPR_BEGIN.length(), expr.length() - ExprConsts.EXPR_END.length());
            result = new ExtractedExprParts();
            if (midExpr.contains(ExprConsts.DELIMITER)) {
                String[] parts = midExpr.split(ExprConsts.DELIMITER);
                Preconditions.checkArgument(parts.length > 0);
                result.partOfTag = parts[0];
                result.partOfContent = parts.length > 1 ? parts[1] : "";
            }
            else
                result.partOfTag = midExpr;
        }
        return result;
    }

    public static void execute(SqlQuery root, Expression sourceExpr, SqlObjectsDbSupportUtils dbSupport, LocalDateTime fixedCurrentDateTime) {
        String result = sourceExpr.getExpr();
        // перед дистилляцией все выражения, содержащие нашу разметку, были преобразованы в деревья,
        // в которых каждый узел содержит только одно выражение с нашей разметкой
        Preconditions.checkNotNull(result);
        Preconditions.checkArgument(result.length() != 0);
        ExtractedExprParts parts = extractExprParts(result);
        if (parts != null) {
            if (!dbSupport.tryReplaceFunctionTagWithNativeFunction(sourceExpr, parts.partOfTag, parts.partOfContent))
                // никаких замен не произошло, попробуем транслировать относительные даты
                dbSupport.relativeDateSupport().tryTranslate(root,
                        sourceExpr, parts.partOfTag, parts.partOfContent,
                        fixedCurrentDateTime);
        }

    }

	public static void execute(SqlQuery root, Expression sourceExpr, SqlObjectsDbSupportUtils dbSupport)
            throws Exception{
        execute(root, sourceExpr, dbSupport, null);
	}

}
