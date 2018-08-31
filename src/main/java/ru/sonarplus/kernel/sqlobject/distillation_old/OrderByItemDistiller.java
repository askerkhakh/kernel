package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.OrderByItem;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import java.util.Set;

public class OrderByItemDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);

	public OrderByItemDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof OrderByItem;
	}
	
	protected boolean isDistillated(DistillerState state)
			throws Exception{
		OrderByItem item = (OrderByItem) state.sqlObject;
		ColumnExpression expr = item.getExpr();
		Preconditions.checkNotNull(expr);
		boolean result = Utils.isTechInfoFilled(checkNotRecordId(expr, state));
		if (!result) {
			internalDistillateObject(new DistillerState(expr, state), COLUMN_EXPRESSION, null);
			result = Utils.isTechInfoFilled(expr);
		}
		return result;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		if (isDistillated(state)) {
			OrderByItem item = (OrderByItem) state.sqlObject;
			ColumnExpression expr = item.getExpr();
			wrapWithUpper(expr, Utils.isCaseSensitive(expr), state);	
		}
		else {
			addNotDistillatedObject(state.sqlObject, state);
		}
		return state.sqlObject;
	}

}
