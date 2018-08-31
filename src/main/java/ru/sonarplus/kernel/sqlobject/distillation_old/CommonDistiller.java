package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.HashSet;
import java.util.Set;

public abstract class CommonDistiller {

	public CommonDistiller() {
	}
	
	public abstract boolean isMy(SqlObject source);
	
	protected void addNotDistillatedObject(SqlObject sqlObject, DistillerState state) {
		state.objectsNotDistilated.add(sqlObject);
	}
	
	public  SqlObject distillate(DistillerState state)
			throws Exception{
		Preconditions.checkNotNull(state.sqlObject);
		return internalDistillate(state);
	}
	
	public static SelectedColumn getColumnThatContainsThisExpr(ColumnExpression expr) {
		Preconditions.checkNotNull(expr);
		SqlObject item = expr;
		do {
			item = item.getOwner();
		}
		while ((item != null) && (!(item instanceof SelectedColumn)) &&
		(!(item instanceof SqlQuery)));
		
		if ((item != null) && (item instanceof SelectedColumn)) {
			return (SelectedColumn) item;
		}
		return null;
			
	}
	
	protected abstract SqlObject internalDistillate(DistillerState state
			)
            throws Exception;
	
	protected void replace(SqlObject what, SqlObject with)
			throws SqlObjectException {
		what.getOwner().replace(what, with);
	}
	
	protected ColumnExpression checkNotRecordId(ColumnExpression expr, DistillerState state) {
		Preconditions.checkArgument(!isRecordId(expr), "В выражении %s использование RecordId недопустимо",
				state.sqlObject.getClass().getSimpleName());
		return expr;
	}
	
	public static boolean isRecordId(SqlObject source) {
		return (source instanceof QualifiedField) && StringUtils.isEmpty(((QualifiedField) source).fieldName); 
	}
	
	public String getMainTableNameOrAlias(SqlObject sqlObject) {
		SqlQuery parent;
		if (sqlObject instanceof Select || sqlObject instanceof DataChangeSqlQuery) {
			parent = (SqlQuery)sqlObject;
		}
		else {
			parent = SqlObjectUtils.getParentQuery(sqlObject);
		}
		Preconditions.checkState(parent != null);
		String result = parent instanceof CursorSpecification || parent instanceof Select ? SqlObjectUtils.getRequestTableAlias(parent) : "";
		if ((result == null) || StringUtils.isEmpty(result)) {
			result = SqlObjectUtils.getRequestTableName(parent);
		}
		return result;
	}
	
	public String fieldAliasPart(QualifiedField qField) {
		String result = qField.alias;
		if (StringUtils.isEmpty(result)) {
			result = getMainTableNameOrAlias(qField);
		}
		return result;
	}
	
	public String fieldAliasPart(Expression expr, String alias) {
		String result = alias;
		if (StringUtils.isEmpty(result)) {
			result = getMainTableNameOrAlias(expr);
		}
		return result;
	}
	
	protected boolean checkIsUpper(SqlObject expr, DistillerState state) {
		return (expr instanceof Expression) && ((Expression) expr).getExpr().equals(state.dbc.dbSupport.dbUpperName() +
				"("+Expression.UNNAMED_ARGUMENT_REF + ")");
	}
	
	protected boolean isNeedUpper(ColumnExpression expr, boolean isCaseSensitive,
			DistillerState state) {
		boolean result = checkIsUpper(expr, state) || ((expr.getOwner() != null) && checkIsUpper(expr.getOwner(), state));
		return (!result) && (!isCaseSensitive);
	}
	
	public void wrapWithUpper(ColumnExpression expr, boolean isCaseSensitive, DistillerState state)
			throws SqlObjectException {
		if (!isNeedUpper(expr, isCaseSensitive, state)) {
			return;
		}
		
		ColumnExpression exprOrg = expr;
		Expression wrapExpr = new Expression(state.dbc.dbSupport.upper(Expression.UNNAMED_ARGUMENT_REF), true);
		SqlObject owner = exprOrg.getOwner();
		if (owner instanceof TupleExpressions) {
			int exprIndex = ((TupleExpressions) owner).indexOf(exprOrg);
			owner.removeItem(exprOrg);
			wrapExpr.insertItem(exprOrg);
			((TupleExpressions)owner).insertItem(wrapExpr, exprIndex);
		}
		else {
			wrapExpr.id = exprOrg.id;
			owner.removeItem(exprOrg);
			exprOrg.id = "";
			wrapExpr.insertItem(exprOrg);
			owner.insertItem(wrapExpr);
		}
		wrapExpr.distTechInfo = exprOrg.distTechInfo;
	
	}
	
	protected static Set<Class<? extends SqlObject>> getAllowedClasses(Class<? extends SqlObject>... types) {
		Set<Class<? extends SqlObject>> result = new HashSet<Class<? extends SqlObject>>();
		for (Class<? extends SqlObject> item: types) {
			result.add(item);
		}
		return result;
	}
	
	
}
