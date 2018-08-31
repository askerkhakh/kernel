package ru.sonarplus.kernel.sqlobject.sqlobject_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.expressions.ExpressionTreeBuilder;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.ArrayList;
import java.util.List;

public class SqlObjectUtils {

    public static boolean isAsterisk(String fieldName) {
        return fieldName.equals("*");
    }

    public static boolean isAsterisk(QualifiedField field) {
        return isAsterisk(field.fieldName);
    }

    public static boolean isAsterisk(ColumnExpression expr) {
        return (expr instanceof QualifiedField) && isAsterisk((QualifiedField) expr);
    }


    public static boolean isRecordId(QualifiedField field) {
        return StringUtils.isEmpty(field.fieldName);
    }

    public static boolean isRecordId(SqlObject source) {
        return (source instanceof QualifiedField) && isRecordId((QualifiedField) source);
    }

    public static boolean isTechParam(Parameter param) {
        Preconditions.checkNotNull(param);
        return param.getClass() == QueryParam.class && StringUtils.isEmpty(param.parameterName);
    }

    public static String getTechParamPrefix(String prefix) {
        StringBuilder result = new StringBuilder();
        if (StringUtils.isEmpty(prefix))
            result.append("TP_");
        else {
            result.append(prefix);
            if (!prefix.endsWith("_")) {
                result.append("_");
            }
        }
        result.append("29JUTPR8W_");
        return result.toString();
    }


    private static SqlObject findTopItem(SqlObject item, Class<? extends SqlQuery> ownerClass,
                                         boolean includeItem, boolean findFirst) {
        if (item != null) {
            SqlObject currentItem = includeItem ? item : item.getOwner();
            while (currentItem != null) {
                if (ownerClass.isAssignableFrom(currentItem.getClass()) && (findFirst || (!findFirst && currentItem.getOwner() == null)))
                    return currentItem;
                currentItem = currentItem.getOwner();
            }
        }
        return null;
    }

    public static SqlQuery findRootQuery(SqlObject item) {
        return (SqlQuery)findTopItem(item, SqlQuery.class, true, false);
    }

    public static SqlQuery getRootQuery(SqlObject item) throws NullPointerException {
        SqlQuery result = findRootQuery(item);
        Preconditions.checkNotNull(result);
        return result;
    }

    public static SqlQuery findParentQuery(SqlObject item) {
        return (SqlQuery)findTopItem(item, SqlQuery.class, false, true);
    }

    public static SqlQuery getParentQuery(SqlObject item) {
        SqlQuery result = findParentQuery(item);
        Preconditions.checkNotNull(result);
        return result;
    }

    public static CursorSpecification findCursorSpecification(SqlObject item) {
        return (CursorSpecification)findTopItem(item, CursorSpecification.class, true, false);
    }

    public static CursorSpecification getCursorSpecification(SqlObject item) {
        CursorSpecification result = findCursorSpecification(item);
        Preconditions.checkNotNull(result);
        return result;
    }

    public static Select findParentSelect(SqlObject item) {
        return (Select)findTopItem(item, Select.class, false, true);
    }

    public static Select getParentSelect(SqlObject item) {
        Select result = findParentSelect(item);
        Preconditions.checkNotNull(result);
        return result;
    }

    private static FromClauseItem findBaseFromItem(Select select) {
        if (select != null) {
            FromContainer fromClause = select.getFrom();
            if (fromClause != null)
                return (FromClauseItem)fromClause.firstSubItem();
        }
        return null;
    }

    private static Source findBaseSource(Select select) {
        FromClauseItem baseFromItem = findBaseFromItem(select);
        if (baseFromItem != null)
            return baseFromItem.getTableExpr().getSource();
        return null;
    }

    public static String getRequestTableName(SqlQuery request) throws IllegalArgumentException{
        if (request instanceof Select) {
            Source source = findBaseSource((Select)request);
            if (source instanceof SourceTable) {
                String table = ((SourceTable) source).table;
                return table == null ? "" : table;
            }
            else if (source instanceof SourceQuery)
                return getRequestTableName(((SourceQuery)source).getSelect());
        }
        else if(request instanceof CursorSpecification)
            return getRequestTableName(((CursorSpecification) request).getSelect());
        else if (request instanceof DataChangeSqlQuery) {
            String table = ((DataChangeSqlQuery) request).table;
            return table == null ? "" : table;
        }
        else
            Preconditions.checkArgument(false);
        return "";
    }

    public static String getRequestTableAlias(SqlQuery request) {
        if (request instanceof Select) {
            FromClauseItem baseFromItem = findBaseFromItem((Select)request);
            if (baseFromItem != null) {
                String alias = baseFromItem.getTableExpr().alias;
                return alias == null ? "" : alias;
            }
        }
        else if (request instanceof CursorSpecification)
            return getRequestTableAlias(((CursorSpecification)request).getSelect());
        else
            Preconditions.checkArgument(false);
        return "";
    }

    public static String getRequestSchemaName(SqlQuery request) {
        if (request instanceof Select) {
            Source source = findBaseSource((Select)request);
            if (source instanceof SourceTable) {
                String schema = ((SourceTable) source).schema;
                return schema == null ? "" : schema;
            }
            else if (source instanceof SourceQuery)
                return getRequestSchemaName(((SourceQuery)source).getSelect());
        }
        else if(request instanceof CursorSpecification)
            return getRequestSchemaName(((CursorSpecification) request).getSelect());
        else if (request instanceof DataChangeSqlQuery) {
            String schema = ((DataChangeSqlQuery) request).schema;
            return schema == null ? "" : schema;
        }
        else
            Preconditions.checkArgument(false);
        return "";
    }

    public static String getRequestFullTableName(SqlQuery request) {
        String schemaName = getRequestSchemaName(request);
        String tableName = getRequestTableName(request);
        return (!StringUtils.isEmpty(schemaName) ? schemaName + "." + tableName : tableName);
    }

    public static String getRequestHint(SqlQuery request) {
        if (request instanceof Select) {
            String hint = ((Select)request).hint;
            return (hint == null) ? "" : hint;
        }
        else if(request instanceof CursorSpecification)
            return getRequestHint(((CursorSpecification) request).getSelect());
        else
            Preconditions.checkArgument(false);
        return "";
    }

    public static ColumnExprTechInfo getTechInfo(ColumnExpression expr) {
        Preconditions.checkNotNull(expr);
        if (expr.distTechInfo == null) {
            expr.distTechInfo = new ColumnExprTechInfo();
        }
        if (expr instanceof QualifiedRField) {

        }
        else if (expr instanceof QualifiedField) {
            if (StringUtils.isEmpty(expr.distTechInfo.dbdFieldName)) {
                expr.distTechInfo.dbdFieldName = ((QualifiedField) expr).fieldName;
            }
            if (StringUtils.isEmpty(expr.distTechInfo.nativeFieldName)) {
                expr.distTechInfo.nativeFieldName = ((QualifiedField) expr).fieldName;
            }
        }
        return expr.distTechInfo;
    }

    protected static ColumnExpression buildTree(Expression expr) {
        return ExpressionTreeBuilder.execute(expr);
    }

    protected static void expressionAsTreeIterate(SqlObject root) {
        List<SqlObject> replaceWhat = new ArrayList<SqlObject>();
        List<SqlObject> replaceWith = new ArrayList<SqlObject>();
        for (SqlObject item: root) {
            Preconditions.checkArgument(item instanceof ColumnExpression);
            if (item instanceof Expression) {
                expressionAsTreeIterate(item);
                SqlObject exprTree = buildTree((Expression) item);
                replaceWhat.add(item);
                replaceWith.add(exprTree);
            }
        }
        for (int i = 0; i < replaceWhat.size(); i++) {
            SqlObject item = replaceWhat.get(i);
            item.getOwner().replace(item, replaceWith.get(i));
        }
    }

    protected static ColumnExpression expressionAsTree(Expression source) {
        Expression copy = (Expression) source.getClone();
        expressionAsTreeIterate(copy);
        return buildTree(copy);
    }

    public static SqlObject buildTreesForExpressions(SqlObject root) {
		if (root instanceof Expression) {
			ColumnExpression newExpr = expressionAsTree((Expression) root);
			root.getOwner().replace(root, newExpr);
			return newExpr;
		}
		else {
			for(SqlObject item: root)
				buildTreesForExpressions(item);
			return root;
		}

	}

    public static ColumnExprTechInfo buildTechInfoForExpr(ColumnExpression expr,
                                                          FieldTypeId type) {
        return buildTechInfoForExpr(expr, type, true);
    }

    public static ColumnExprTechInfo buildTechInfoForExpr(ColumnExpression expr,
                                                          FieldTypeId type, Boolean isCaseSensitive) {
        Preconditions.checkNotNull(expr);
        Preconditions.checkArgument((expr instanceof ColumnExpression) &&
                expr.getClass() != ValueRecId.class &&
                expr.getClass() != QualifiedRField.class &&
                !(expr instanceof QualifiedField && (isAsterisk((QualifiedField)expr) || isRecordId((QualifiedField)expr))) &&
                !(expr instanceof Parameter && ((Parameter)expr).isContainedInParamsClause())
        );

        ColumnExprTechInfo result = new ColumnExprTechInfo();

        FieldTypeId valueType = type;
        if (expr instanceof QualifiedField) {
            result.nativeFieldName = ((QualifiedField) expr).fieldName;
            result.tableExprName = ((QualifiedField) expr).alias;
        }
        else if (expr instanceof Value) {
             if (valueType == FieldTypeId.tid_UNKNOWN)
                valueType = ((Value) expr).getValueType();
        }
        result.fieldTypeId = valueType;
        result.caseSensitive = isCaseSensitive;
        result.techInfoPrepared = true;
        expr.distTechInfo = result;
        return result;
    }

    public static String getCSBFieldName(ColumnExpression expr) {
        return getTechInfo(expr).csbNativeFieldName;
    }

    public static int getBytesForCode(ColumnExpression expr) {
        return getTechInfo(expr).bytesForCode;
    }

    public static boolean isPureAsterisk(SqlObject source) {
        return (source != null) && (source.getClass() == Expression.class) &&
                ((Expression)source).isPureAsterisk();
    }

    public static class CTE {

        protected static class CTESearchContext {
            public boolean isInner;
        }

        public static CommonTableExpression findCTE(String alias, Select select) {
            return findCTE(alias, select, new CTESearchContext());
        }

        public static CommonTableExpression findCTE(FromClauseItem from) {
            return findCTE(from, new CTESearchContext());
        }

        protected static CommonTableExpression findCTE(FromClauseItem from, CTESearchContext context) {
            Source source =
                    Preconditions.checkNotNull(
                            Preconditions.checkNotNull(
                                    Preconditions.checkNotNull(from).getTableExpr()
                            ).getSource()
                    );
            if (source instanceof SourceTable)
                return findCTE(source.getTable(), SqlObjectUtils.getParentSelect(from), context);
            else
                return null;
        }

        protected static CommonTableExpression findCTE(String alias, Select select, CTESearchContext context) {
            Preconditions.checkNotNull(select);
            Preconditions.checkNotNull(alias);
            Preconditions.checkArgument(!alias.isEmpty());

            context.isInner = false;
            // запрос может содержаться внутри CTE
            CommonTableExpression result = getParentCTE(select);
            if (result != null) {
                // в этом случае ссылаться можно только на данное CTE
                //   или CTE, определенные ранее данного
                // если так, то ...
                if (!result.alias.equals(alias))
                    // ссылка может быть на CTE, определённый ранее найденного
                    return findInCTEs(alias, getParentCTEs(result), result);
                else
                    // ссылка может быть рекурсивной, на уже найденный CTE, ссылка на который в Result
                    return result;
            }

            //  в противном случае это может быть подзапрос в теле корневого запроса (или сам корневой запрос),
            //  содержащего раздел WITH. В этом случае мы можем ссылаться на любой CTE (если таковые есть) }
            result = findInCTEs(alias, getTopMostSelectFromItem(select).findWith(), null);
            context.isInner = result != null;
            return result;
        }

        protected static Select getTopMostSelectFromItem(SqlObject item) {
            if (item == null)
                return null;
            Select result = item instanceof Select ? (Select)item : null;
            SqlObject owner = item.getOwner();
            while (owner != null) {
                if (owner instanceof Select)
                    result = (Select) owner;
                owner = owner.getOwner();
            }
            return result;
        }

        protected static CommonTableExpression findInCTEs(String alias, CTEsContainer ctes, CommonTableExpression beforeCTE) {
            if (ctes == null)
                return null;
            CommonTableExpression cte;
            for (SqlObject item: ctes) {
                cte = (CommonTableExpression) item;
                if (cte == beforeCTE)
                    return null;
                else if (cte.alias.equals(alias))
                    return cte;
            }
            return null;
        }

        protected static CTEsContainer getParentCTEs(CommonTableExpression cte) {
            if (cte == null)
                return null;
            return (CTEsContainer) Preconditions.checkNotNull(cte.getOwner());
        }

        protected static CommonTableExpression getParentCTE(Select select) {
            SqlObject item = select.getOwner();
            while (item != null)
                if (item instanceof CommonTableExpression)
                    return (CommonTableExpression) item;
                else
                    item = item.getOwner();
            return null;
        }
    }
}
