package ru.sonarplus.kernel.sqlobject.distillation;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.distillation_old.Utils;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExpressionException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Map;

public class DistillerUtils {

    public static SqlObject replace(SqlObject source, SqlObject target)
            throws SqlObjectException {
        Preconditions.checkNotNull(target);
        String sourceId = Preconditions.checkNotNull(source) instanceof ColumnExpression ? ((ColumnExpression)source).id : null;
        if (source.getOwner() != null)
            source.getOwner().replace(source, target);
        if (target instanceof ColumnExpression)
            ((ColumnExpression)target).id = sourceId;
        return target;
    }

    public static boolean containsQRNames(SqlObject root) {
        if (root  == null)
            return false;
        if (root.getClass() == QualifiedRField.class)
            return true;
        if (root.getClass() == QualifiedField.class && (SqlObjectUtils.isRecordId((QualifiedField)root) || SqlObjectUtils.isAsterisk((QualifiedField)root)))
            return true;
        if (root.getClass() == Expression.class) {
            String expr = ((Expression) root).getExpr();
            if (!StringUtils.isEmpty(expr) && expr.contains(ExprConsts.QRNAME))
                return true;
        }
        for(SqlObject item: root)
            if (containsQRNames(item))
                return true;
        return false;
    }

    public static ColumnExpression applyUpper(ColumnExpression expr, DistillerParams params)
            throws SqlObjectException {
        ColumnExpression result = expr;
        String sUpper = params.dbSupport.upper(Expression.UNNAMED_ARGUMENT_REF);
        if (result != null && !(result instanceof Expression && ((Expression)result).getExpr().equals(sUpper))) {
            result = new Expression(sUpper, true);
            SqlObject owner = expr.getOwner();
            if (owner != null) {
                if (owner instanceof TupleExpressions) {
                    int exprIndex = ((TupleExpressions) owner).indexOf(expr);
                    owner.removeItem(expr);
                    ((TupleExpressions)owner).insertItem(result, exprIndex);
                }
                else {
                    result.id = expr.id;
                    expr.id = null;
                    owner.removeItem(expr);
                    owner.insertItem(result);
                }
            }
            result.insertItem(expr);
            result.distTechInfo = expr.distTechInfo;
        }
        return result;
    }

    public static ColumnExpression applyUpperIfNotCaseSensitive(ColumnExpression expr, DistillerParams params)
            throws SqlObjectException {
        ColumnExpression result = expr;
        if (result != null) {
            ColumnExprTechInfo spec = result.distTechInfo;
            if (spec != null && !spec.caseSensitive)
                result = applyUpper(expr, params);
        }
        return result;
    }

    public static String calcQFieldAliasPart(SqlObject item, String defaultAlias, SqlQuery parent){
        String result = defaultAlias;
        if (StringUtils.isEmpty(result)) {
            //SqlQuery parent = SqlObjectUtils.getParentQuery(item);
            result = parent instanceof CursorSpecification || parent instanceof Select ? SqlObjectUtils.getRequestTableAlias(parent) : null;
            if (StringUtils.isEmpty(result))
                result = SqlObjectUtils.getRequestTableName(parent);
        }
        return result;
    }

    public static String calcQFieldAliasPart(QualifiedField qfield, SqlQuery parent)
            throws SqlObjectException{
        return calcQFieldAliasPart(qfield, qfield.alias, parent);
    }

    protected static boolean isSubSelectInFromClause(Select select) {
        SqlObject item = select;
        while ((item.getOwner() != null) &&
                !(item.getOwner() instanceof CursorSpecification) &&
                !(item.getOwner() instanceof CommonTableExpression)) {
            if (item.getOwner() instanceof SourceQuery) {
                return true;
            }
            item = item.getOwner();
        }
        return false;
    }

    public static void checkPureAsteriskPossibleHere(Expression asterisk) throws SqlObjectException {
        if (asterisk.getOwner() instanceof SelectedColumn) {
            SqlObject parentQuery = SqlObjectUtils.getParentQuery(asterisk);
            // не позволяем настоящую звёздочку в корневом запросе, иначе как будем настраивать поля набора данных?
            if (parentQuery.getOwner() == null)
                throw new DistillationException("Использование sql-выражения '*' недопустимо в корневом sqlobject-запросе");
            if (isSubSelectInFromClause((Select)parentQuery))
                throw new DistillationException("Использование sql-выражения '*' допускается только в подзапросах разделов FROM");
        }
        else {
            if (asterisk.getOwner() != null && asterisk.getOwner().getClass() != Expression.class)
                throw new DistillationException(
                        String.format(
                                "Sql-выражение '*' должно содержаться непосредственно в определении колонки или быть аргументом другого sql-выражения. Содержится в '%s'",
                                asterisk.getOwner().getClass().getName()
                        )
                );
        }
    }

    public static boolean columnContainsSpecialQField(SelectedColumn column)
            throws DistillationException {
        ColumnExpression expr = column.getColExpr();
        if (expr == null)
            throw new DistillationException("У колонки не задано выражение");
        else if (expr instanceof QualifiedField)
            return SqlObjectUtils.isAsterisk(expr) || SqlObjectUtils.isRecordId(expr);
        else
            return false;
    }

    public static void checkQFieldUsage(QualifiedField qfield, SqlQuery parent)
            throws DistillationException {
        Select select;
        if (SqlObjectUtils.isRecordId(qfield)) {
            //`идентификатор записи`
            Preconditions.checkNotNull(qfield.getOwner());
            if (qfield.getOwner() instanceof SelectedColumn) {
                if (parent == null)
                    select = Preconditions.checkNotNull(SqlObjectUtils.getParentSelect(qfield.getOwner()));
                else
                    select = (Select)parent;
                SqlObject owner = select.getOwner();
                if (owner == null || owner instanceof SourceQuery)
                    return;
                else
                    throw new DistillationException(String.format(
                            "Запрос с колонкой, содержащей выражение 'RecordId' (разворачиваемое в набор полей) не может содержаться в '%s', только в разделе FROM или быть корневым",
                            owner.getClass().getName()
                    ));
            }
            else if (qfield.getOwner() instanceof PredicateComparison)
                // TODO RecordId пока позволяем в сравнении
                return;
            else
                throw new DistillationException(String.format(
                        "Выражение 'RecordId' (разворачиваемое в набор полей) не может содержаться в '%s', только в колонке корневого запроса или подзапроса в разделе FROM",
                        qfield.getOwner().getClass().getName()
                ));
        }
        else if (SqlObjectUtils.isAsterisk(qfield)) {
            // `звёздочка`, разворачиваемая в перечень полей
            Preconditions.checkNotNull(qfield.getOwner());
            if (qfield.getOwner() instanceof SelectedColumn) {
                if (parent == null)
                    select = SqlObjectUtils.getParentSelect(qfield.getOwner());
                else
                    select = (Select)parent;
                SqlObject owner = select.getOwner();
                if (owner == null || owner.getClass() == CursorSpecification.class || owner instanceof SourceQuery )
                    return;
                else
                    throw new DistillationException(String.format(
                            "Запрос с колонкой, содержащей выражение '*' (разворачиваемое в набор полей) не может содержаться в '%s', только в разделе FROM или быть корневым",
                            owner.getClass().getName()
                    ));
            }
            else
                throw new DistillationException(String.format(
                        "Выражение '*' (разворачиваемое в набор полей) не может содержаться в '%s', только в колонке корневого запроса или подзапроса в разделе FROM",
                        qfield.getOwner().getClass().getName()
                ));
        }
    }

    public static void checkParamRefUsage(ParamRef paramRef, QueryParam param)
            throws DistillationException {
        Preconditions.checkNotNull(param);
        if (!param.isRecId())
            return;
        else if (paramRef.getOwner() instanceof PredicateComparison)
            // TODO RecordId пока позволяем в сравнении
            return;
        else {
            Preconditions.checkNotNull(paramRef.getOwner());
            throw new DistillationException(String.format(
                    "Параметр '%', содержащий значение RecordId не может содержаться в '%s'",
                    paramRef.parameterName,
                    paramRef.getOwner().getClass().getName()
                    ));
        }
    }

    public static int getUnnamedRefCount(String str, Map<String, Integer> refCounts) throws ExpressionException {
        String upper = str.toUpperCase();
        Integer result = refCounts.get(upper);
        if (result == null) {
            int res = ExprUtils.getUnnamedRefCount(str);
            refCounts.put(upper, Integer.valueOf(res));
            return res;
        }
        return result;
    }

    public static boolean isParamValueRecId(SqlObject item, DistillationState state)
            throws DistillationException{
        if (item == null || item.getClass() != ParamRef.class)
            return false;
        QueryParam queryParam = state.getRoot().getParams().findExistingParam(((ParamRef)item).parameterName);
        return queryParam.isRecId();
    }
/*
    public static Select getTopSelectOfUnions(Select select) {
        Preconditions.checkNotNull(select);
        if (select.owner == null || select.owner.getClass() != UnionItem.class)
            return select;

        SqlObject current = select.owner;
        while (true) {
            if (current == null || (current.getClass() == Select.class && (current.owner == null || (current.owner.getClass() != UnionItem.class))))
                break;
            current = current.owner;
        }
        Preconditions.checkState(current != null && current.getClass() == Select.class);
        return (Select) current;
    }
*/
}
