package ru.sonarplus.kernel.sqlobject.merge_utils;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.Set;
import java.util.TreeSet;

public class JoinsConvertor {

    public static void convertInnerJoinsToLeftJoins(Select select)
            throws CloneNotSupportedException, SqlObjectException {
        ExtractJoiningFieldsContext extractContext = new ExtractJoiningFieldsContext();
        FromContainer fromClause = select.getFrom();
        FromClauseItem from = null;
        // из условий подлитых табличных выражений получим перечень копий подлитых полей связи
        for(SqlObject item: fromClause) {
            if (item == select.getFrom().firstSubItem())
                continue;
            from = (FromClauseItem) item;
            Preconditions.checkNotNull(from.getJoin()).joinType = Join.JoinType.LEFT;
            extractJoiningFields(from, extractContext);
        }
        if (extractContext.joiningFields != null && extractContext.joiningFields.isHasChilds()) {
            Conditions whereOld = select.findWhere();
            Conditions whereNew = null;
            // если в запросе есть непустой блок условий...
            if (whereOld != null && !whereOld.isEmpty()) {
                //... создадим новый блок условий, объединив его со старым по AND
                whereNew = new Conditions(Conditions.BooleanOp.AND);
                whereNew.insertItem(whereOld);
                select.setWhere(whereNew);
            }
            else
                whereNew = select.newWhere();
            // в созданном блоке условий создадим условия "not is null" для полученных полей
            PredicateIsNull predicate = null;
            while (extractContext.joiningFields.itemsCount() != 0) {
                predicate =
                        new PredicateIsNull(whereNew,
                                (QualifiedField)extractContext.joiningFields.firstSubItem())
                                .setNot(true);
                predicate.isRaw = true; // чтобы условие не превратилось в сравнение с нулевым значением
            }
        }
    }

    protected static void extractJoiningFields(FromClauseItem from, ExtractJoiningFieldsContext extractContext)
            throws CloneNotSupportedException, SqlObjectException {

        reqursiveExtractJoiningFields(
                Preconditions.checkNotNull(
                        Preconditions.checkNotNull(from.getJoin())
                                .getJoinOn()
                ),
                from.getAliasOrName(), extractContext);
    }

    protected static void reqursiveExtractJoiningFields(SqlObject root, String joinedTableAlias, ExtractJoiningFieldsContext extractContext)
            throws CloneNotSupportedException, SqlObjectException {

        for(SqlObject item: root)
          reqursiveExtractJoiningFields(item, joinedTableAlias, extractContext);
        if (root instanceof ColumnExpression)
            extractContext.tryAddFieldsFrom((ColumnExpression)root, joinedTableAlias);
    }

    protected static class ExtractJoiningFieldsContext {
        public TupleExpressions joiningFields = null;
        public Set<String> extractedFieldNames = null;

        public void tryAddFieldsFrom(ColumnExpression colExpr, String joinedTableAlias)
                throws CloneNotSupportedException, SqlObjectException {

            if (colExpr instanceof QualifiedField)
                addFieldsFrom((QualifiedField)colExpr, joinedTableAlias);
            else if (colExpr instanceof Expression)
                addFieldsFrom(((Expression) colExpr).getExpr(), joinedTableAlias);
        }

        protected Set<String> ensureExtractedFieldNames() {
            if (extractedFieldNames == null)
                extractedFieldNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            return extractedFieldNames;
        }

        protected TupleExpressions ensureJoiningFields() {
            if (joiningFields == null)
                joiningFields = new TupleExpressions();
            return joiningFields;
        }

        protected void addFieldsFrom(QualifiedField qfield, String joinedTableAlias)
                throws CloneNotSupportedException, SqlObjectException {

            String strQField = qfield.getQName().qualifiedNameToString();
            QualifiedField qfieldClone;
            if (qfield.alias.compareToIgnoreCase(joinedTableAlias) == 0 &&
                    !ensureExtractedFieldNames().contains(strQField)) {
                ensureExtractedFieldNames().add(qfield.getQName().qualifiedNameToString());
                qfieldClone = (QualifiedField)qfield.clone();
                qfieldClone.id = "";
                ensureJoiningFields().insertItem(qfieldClone);
            }
        }

        protected void addFieldsFrom(String expr, String joinedTableAlias)
                throws SqlObjectException {

            String strQName = null;
            for(QualifiedName qname: ExprUtils.exprExtractQRNames(expr))
                if (qname.alias.compareToIgnoreCase(joinedTableAlias) == 0) {
                    strQName = qname.qualifiedNameToString();
                    if (!ensureExtractedFieldNames().contains(strQName)) {
                        ensureExtractedFieldNames().add(qname.qualifiedNameToString());
                        ensureJoiningFields().insertItem(new QualifiedRField(qname.alias, qname.name));
                    }
                }

            for(QualifiedName qname: ExprUtils.exprExtractQNames(expr))
                if (qname.alias.compareToIgnoreCase(joinedTableAlias) == 0) {
                    strQName = qname.qualifiedNameToString();
                    if (!ensureExtractedFieldNames().contains(strQName)) {
                        ensureExtractedFieldNames().add(qname.qualifiedNameToString());
                        ensureJoiningFields().insertItem(new QualifiedField(null, qname.alias, qname.name));
                    }
                }
        }
    }

}
