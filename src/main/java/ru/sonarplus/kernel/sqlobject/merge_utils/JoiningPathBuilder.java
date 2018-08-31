package ru.sonarplus.kernel.sqlobject.merge_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.RenamingDict;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExpressionTreeBuilder.ArgRefs;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Построитель "пути слияния" для элемента раздела FROM.
 * Под путём слияния здесь понимается сериализованная (в строку) информация:
 * - о подливаемом табличном выражении;
 * - о родительской совокупности табличных выражений;
 * - о связи подливаемого выражения с родителями.
 * путь слияния выглядит следующим образом:
 * "(PARENT1)(PARENT2)...(PARENTN)(LEFT)(AND:(условие1)(условие2)...)(JOINED_TABLE_NAME)"
 * где (PARENT1)(PARENT2)...(PARENTN) - перечень слитых табличных выражений, с которыми связана подлитая таблица
 *   каждый из PARENT-ов представляет собой аналогичную строку.
 *   состав PARENT-ов определяется на основании алиасных частей квалифицированных полей в условиях слияний.
 * (LEFT) - тип слияния (RIGHT, INNER...);
 * (AND:...) - при наличии более чем одного условия слияния;
 * (условие1) - отдельное условие слияния - скобка (набор условий) или сравнение;
 * (JOINED_TABLE_NAME) - имя (не алиас) подливаемой таблицы.
 *
 * Путь слияния в таком виде используется для сравнения элементов From объединяемых запросов
 *
 * Примеры:
 * Для первого элемента From, определяющего основную таблицу запроса TABLE с алиасом, к примеру TABLE_0
 * путь слияния будет "(TABLE)" - это соответствует (JOINED_TABLE_NAME), т.е. нет родительских таблиц и условий связи.
 *
 * для запроса
 * select * from table1 table1_1
 * left join table2 table2_2 on table2_2.field21 = table1_1.field11 or table2_2.field22 = table1_1.field12
 *
 * путь слияния первого элемента будет "(TABLE1)"
 * для второго элемента "(TABLE1)(LEFT)(AND:(FIELD11 EQUAL FIELD21)(FIELD12 EQUAL FIELD22))(TABLE2)"
 *
 * - для построения используются имена таблиц;
 * - при сериализации условий сравнения - поле родительской таблицы будет слева, подлитой - справа;
 * - наборы условий (скобки) - сортируются, чтобы при разном порядке следования в скобках одинаковых условий
 *   скобки сериализовались одинаково
 */
public class JoiningPathBuilder {

    private RenamingDict renamingCTEs;
    private Select select;

    public JoiningPathBuilder() {
    }

    public String execute(FromClauseItem from, RenamingDict renamingCTEs)
            throws SqlObjectException {
        this.renamingCTEs = renamingCTEs;
        this.select =
                (Select)Preconditions.checkNotNull(
                        Preconditions.checkNotNull(Preconditions.checkNotNull(from).getOwner() // FromContainer
                        ).getOwner() // Select
                );
        StringBuilder sb = new StringBuilder();
        String mainTable = SqlObjectUtils.getRequestTableName(select);
        Preconditions.checkArgument(!StringUtils.isEmpty(mainTable));
        internalExecute(from, sb, mainTable);
        return sb.toString().toUpperCase();
    }

    protected void internalExecute(FromClauseItem from, StringBuilder sb, String mainTable)
            throws SqlObjectException {
        FromContainer fromClause = (FromContainer) from.getOwner();
        if (from == fromClause.firstSubItem()) {
            sb.append(SourceSerializer.sourceToString(from, renamingCTEs));
            return;
        }

        FromClauseItem[] parentFroms = getParentFroms(from, mainTable);
        for(FromClauseItem parent: parentFroms) {
            sb.append('(');
            internalExecute(parent, sb, mainTable);
            sb.append(')');
        }

        sb.append('(' + Preconditions.checkNotNull(from.getJoin()).joinType.name() + ')');
        sb.append(PredicateSerializer.predicateToString(from.getJoin().getJoinOn(), from.getAliasOrName()));
        sb.append(SourceSerializer.sourceToString(from, renamingCTEs));
    }

    protected static FromClauseItem[] getParentFroms(FromClauseItem from, String mainTable)
            throws SqlObjectException {
        FromContainer fromClause = (FromContainer) from.getOwner();
        List<FromClauseItem> parents = new ArrayList<FromClauseItem>();
        for(String alias: AliasesChecker.getParentAliases(from, mainTable))
            parents.add(Preconditions.checkNotNull(fromClause.findItem(alias), String.format("Не найден элемент раздела From с алиасом '%s'", alias)));
        return parents.toArray(new FromClauseItem[0]);
    }


    protected static class AliasesChecker {

        public static String[] getParentAliases(FromClauseItem from, String mainTable) {
            List<String> aliases = new ArrayList<String>();
            internalGetParentAliases(Preconditions.checkNotNull(from.getJoin().getJoinOn()), aliases, from.getAliasOrName(), mainTable);
            aliases.sort(String::compareToIgnoreCase);
            return aliases.toArray(new String[0]);
        }

        public static boolean isJoined(ColumnExpression expr, String joinedAlias) {
            Preconditions.checkNotNull(expr);
            if (expr instanceof QualifiedField)
                return isJoined((QualifiedField)expr, joinedAlias);
            else if (expr instanceof Expression)
                return isJoined((Expression)expr, joinedAlias);
            else
                Preconditions.checkArgument(false, expr.getClass().getName());
            return false;
        }

        protected static boolean isJoined(QualifiedField qfield, String joinedAlias) {
            return qfield.alias.compareToIgnoreCase(joinedAlias) == 0;
        }

        protected static boolean isJoined(Expression expr, String joinedAlias) {
            for (QualifiedName qname: ExprUtils.exprExtractQRNames(expr.getExpr()))
                if (qname.alias.compareToIgnoreCase(joinedAlias) == 0)
                    return true;
            for (QualifiedName qname: ExprUtils.exprExtractQNames(expr.getExpr()))
                if (qname.alias.compareToIgnoreCase(joinedAlias) == 0)
                    return true;
            for (SqlObject item: expr)
                if (isJoined((ColumnExpression)item, joinedAlias))
                    return true;
            return false;
        }

        protected static void internalGetParentAliases(SqlObject item, List<String> aliases, String joinedAlias, String mainTable) {
            if (item instanceof ColumnExpression) {
                if (item instanceof QualifiedField)
                    internalGetParentAliases((QualifiedField) item, aliases, joinedAlias, mainTable);
                else if (item instanceof Expression)
                    internalGetParentAliases((Expression) item, aliases, joinedAlias, mainTable);
                else
                    Preconditions.checkState(false);
            }
            for(SqlObject child: item)
                internalGetParentAliases(child, aliases, joinedAlias, mainTable);
        }

        protected static void internalGetParentAliases(QualifiedField qfield, List<String> aliases, String joinedAlias, String mainTable) {
            tryAddAlias(qfield.alias, aliases, joinedAlias, mainTable);
        }

        protected static void internalGetParentAliases(Expression expr, List<String> aliases, String joinedAlias, String mainTable) {
            for (QualifiedName qname: ExprUtils.exprExtractQRNames(expr.getExpr()))
                tryAddAlias(qname.alias, aliases, joinedAlias, mainTable);
            for (QualifiedName qname: ExprUtils.exprExtractQNames(expr.getExpr()))
                tryAddAlias(qname.alias, aliases, joinedAlias, mainTable);
        }

        protected static void tryAddAlias(String alias, List<String> aliases, String joinedAlias, String mainTable) {
            if (alias.compareToIgnoreCase(joinedAlias) != 0) {
                String upperAlias;
                if (alias.isEmpty())
                    upperAlias = mainTable.toUpperCase();
                else
                    upperAlias = alias.toUpperCase();
                if (!aliases.contains(upperAlias))
                    aliases.add(upperAlias);
            }
        }
    }

    protected static class ColumnExpressionSerializer {

        public static String columnExpressionToString(ColumnExpression expr)
                throws SqlObjectException {

            if (expr instanceof QualifiedField) {
                // алиасная часть поля не нужна, т.к. уже определились, к какой таблице оно относится
                if (expr instanceof QualifiedRField)
                    return "r`" + ((QualifiedField) expr).fieldName;
                else
                    return ((QualifiedField) expr).fieldName;
            }
            else if (expr instanceof ValueConst)
                return expr.toString();
            else if (expr instanceof Parameter) {
                Parameter p = (Parameter) expr;
                // непонятно, что здесь делать с "техническими", т.е. безымянными параметрами
                Preconditions.checkState(!StringUtils.isEmpty(p.parameterName));
                return p.parameterName;
            }
            else if (expr instanceof Expression)
                return expressionToString((Expression)expr);
            else
                Preconditions.checkArgument(false, "В алгоритме формирования пути слияния для условий сравнения поддержаны только имена полей, параметры, значения и выражения");
            return null;
        }

        protected static String expressionToString(Expression expr)
                throws SqlObjectException {
            // собираем дерево выражения в одну строку.
            // это #BAD#, т.к. фактически дублируем алгортим SqlObjectsConvertor.convertExpression()
            // за исключением того, что не выполняем конвертацию в sql, а берём выражение "как есть"
            StringBuilder sb = new StringBuilder();
            String strExpr = Preconditions.checkNotNull(expr.getExpr());
            int pos = 0;
            int posBuildFrom = pos;
            int unnamedRefIndex = 0;
            boolean isLastPartBuilded = false;
            ArgRefs argRefs = ArgRefs.NOTHING;
            String token;
            ColumnExpression child;
            while (pos < strExpr.length()) {
                switch (strExpr.charAt(pos)) {
                    case '\'':
                        pos = ExprUtils.getPosAfterLiteral(strExpr, pos);
                        isLastPartBuilded = false;
                        break;
                    case Expression.CHAR_BEFORE_TOKEN:
                        sb.append(strExpr, posBuildFrom, pos);
                        isLastPartBuilded = true;
                        token = ExprUtils.getToken(strExpr, pos);
                        pos += token.length();
                        posBuildFrom = pos;
                        if (token.equals(Expression.UNNAMED_ARGUMENT_REF)) {
                            // безымянная ссылка
                            Preconditions.checkState(argRefs == ArgRefs.NOTHING || argRefs == ArgRefs.UNNAMED);
                            sb.append(columnExpressionToString((ColumnExpression) expr.getItem(unnamedRefIndex)));
                            unnamedRefIndex++;
                            argRefs = ArgRefs.UNNAMED;
                        }
                        else {
                            // именованая ссылка
                            Preconditions.checkState(argRefs == ArgRefs.NOTHING || argRefs == ArgRefs.NAMED);
                            sb.append(columnExpressionToString(expr.findById(token.substring(1))));
                            argRefs = ArgRefs.NAMED;
                        }
                        break;
                    default:
                        pos++;
                        isLastPartBuilded = false;
                }
            }
            if (!isLastPartBuilded)
                sb.append(strExpr, posBuildFrom, pos);
            return sb.toString();
        }
    }

    protected static class PredicateSerializer {

        public static String predicateToString(Predicate predicate, String joinedAlias)
                throws SqlObjectException {
            Preconditions.checkNotNull(predicate);
            Preconditions.checkNotNull(joinedAlias);
            Preconditions.checkArgument(!joinedAlias.isEmpty());
            return Preconditions.checkNotNull(internalPredicateToString(predicate, joinedAlias));
        }

        protected static String internalPredicateToString(Predicate predicate, String joinedAlias)
                throws SqlObjectException {
            if (predicate instanceof PredicateComparison)
                return comparisonToString((PredicateComparison)predicate, joinedAlias);
            else if (predicate instanceof Conditions)
                return bracketToString((Conditions)predicate, joinedAlias);
            Preconditions.checkArgument(false, "В алгоритме формирования пути слияния поддержаны только условия сравнения");
            return null;
        }

        protected static String comparisonToString(PredicateComparison predicate, String joinedAlias)
                throws SqlObjectException {
            ComparisonOperands operands = getComparisonOperands(predicate, joinedAlias);
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            if (predicate.not)
                sb.append("NOT:");
            sb.append(ColumnExpressionSerializer.columnExpressionToString(operands.left))
              .append(' ')
              .append(predicate.comparison.toString())
              .append(' ')
              .append(ColumnExpressionSerializer.columnExpressionToString(operands.right));
            sb.append(')');
            return sb.toString();
        }

        protected static class ComparisonOperands {
            ColumnExpression left, right;
        }

        protected static ComparisonOperands getComparisonOperands(PredicateComparison predicate, String joinedAlias) {
            ComparisonOperands operands = new ComparisonOperands();
            ColumnExpression left = predicate.getLeft(), right = predicate.getRight();
            if (AliasesChecker.isJoined(left, joinedAlias))
                operands.right = left;
            else
                operands.left = left;

            if (AliasesChecker.isJoined(right, joinedAlias)) {
                Preconditions.checkState(operands.right == null);
                operands.right = right;
            }
            else {
                Preconditions.checkState(operands.left == null);
                operands.left = right;
            }

            Preconditions.checkState(operands.left != null && operands.right != null);
            return operands;
        }

        protected static String bracketToString(Conditions predicate, String joinedAlias)
                throws SqlObjectException {
            if (predicate.isEmpty())
                return null;
            List<String> conditions = new ArrayList<String>();
            String strItem;
            for(SqlObject item: predicate) {
                strItem = internalPredicateToString((Predicate)item, joinedAlias);
                if (!StringUtils.isEmpty(strItem))
                    conditions.add(strItem);
                    //conditions.add('(' + strItem + ')');
            }
            if (conditions.size() == 0)
                return null;
            else if (conditions.size() == 1)
                return conditions.get(0);
            else {
                // отсортируем сериализованный набор условий
                conditions.sort(null);
                StringBuilder sb = new StringBuilder();
                sb.append('(');
                if (predicate.not)
                    sb.append("NOT:");
                sb.append(predicate.booleanOp.toString());
                sb.append(':');
                sb.append(StringUtils.join(conditions, ""));
                sb.append(')');
                return sb.toString();
            }
        }
    }

    protected static class SourceSerializer {

        private static int requestCount = 0;
        private static final String REQUEST_SELECT_PREFIX = "IWXPHVRCF_SELECT_";

        public static String sourceToString(FromClauseItem from, RenamingDict renamingCTEs) {
            Source source = Preconditions.checkNotNull(Preconditions.checkNotNull(from.getTableExpr()).getSource());
            if (source instanceof SourceTable)
                return sourceTableAsString((SourceTable) source, renamingCTEs);
            else if (source instanceof SourceQuery)
                return sourceQueryAsString((SourceQuery) source);
            else
                Preconditions.checkState(false);
            return null;
        }

        protected static String sourceTableAsString(SourceTable source, RenamingDict renamingCTEs) {
            CommonTableExpression cte = SqlObjectUtils.CTE.findCTE((FromClauseItem) source.getOwner().getOwner());
            if ((cte != null) && (renamingCTEs != null))
                return renamingCTEs.rename(source.getTable());
            return source.getTable();
        }

        protected static String sourceQueryAsString(SourceQuery source) {
            // считаем подзапросы в разделах From всегда разными и в качестве их представления в пути слияния
            // создаём уникальные метки
            // может быть потом научимся сравнивать подзапросы...
            return REQUEST_SELECT_PREFIX + Integer.toString(requestCount++);
        }
    }
}
