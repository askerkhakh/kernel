package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Set;

public class SelectedColumnDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);
	
	public SelectedColumnDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof SelectedColumn;
	}
	
	protected boolean isColExprDistillated(ColumnExpression expr, DistillerState state)
			throws Exception{
		Preconditions.checkNotNull(expr);
		boolean result = Utils.isTechInfoFilled(expr);
		if (!result) {
			result = internalDistillateObject(new DistillerState(expr, state), COLUMN_EXPRESSION, null) != null;
		}
		return result;
	}
	
	protected boolean isNeedColumnAlias(SelectedColumn column) {
		SqlQuery parentQuery = SqlObjectUtils.getParentQuery(column);
	    // алиасы колонок имеют смысл только для корневых запросов или подзапросов в разделе From
		return (parentQuery.getOwner() == null) || (parentQuery.getOwner() instanceof CursorSpecification)
				|| (parentQuery.getOwner() instanceof SourceQuery);
	}
	
	protected void setAlias(DistillerState state) {
		SelectedColumn column = (SelectedColumn) state.sqlObject;
		if (!isNeedColumnAlias(column)) {
		    // если алиасы колонок не требуются - убираем их для сокращения текста запроса
			column.alias = "";
		}
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		SelectedColumn column = (SelectedColumn) state.sqlObject;
		ColumnExpression expr = column.getColExpr();
		if (isRecordId(expr) || SqlObjectUtils.isAsterisk(expr)) {
			internalDistillateObject(new DistillerState(expr, state), COLUMN_EXPRESSION, null);
			return null;
		}
		else {
			if (isColExprDistillated(expr, state)) {
				setAlias(state);
			}
			else {
				addNotDistillatedObject(column, state);
			}
			return column;
		}
	}

}
