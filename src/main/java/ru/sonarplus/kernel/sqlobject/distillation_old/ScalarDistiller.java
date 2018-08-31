package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.HashSet;
import java.util.Set;

public class ScalarDistiller extends CommonBaseDistiller {

	public ScalarDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof Scalar;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		SqlObject result = state.sqlObject;
		Scalar scalar = (Scalar) result; 
		Select select = scalar.findSelect();
		Preconditions.checkNotNull(select);
		Set<Class<? extends SqlObject>> allowed = new HashSet<Class<? extends SqlObject>>();
		allowed.add(Select.class);
		internalDistillateObject(new DistillerState(select, state), allowed, null);
		int columnsCount = select.getColumns().itemsCount();
		Preconditions.checkArgument(columnsCount == 1,
				"В скалярном запросе должна быть ровно одна колонка, присутствует %d",
				columnsCount);
		ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(((SelectedColumn) select.getColumns().firstSubItem()).getColExpr()).clone();
		techInfo.nativeFieldName = "";
		techInfo.dbdFieldName = "";
		scalar.distTechInfo = techInfo;
		return result;
	}

}
