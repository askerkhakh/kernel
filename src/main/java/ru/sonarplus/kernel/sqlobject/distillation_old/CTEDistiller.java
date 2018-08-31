package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.CTEsContainer;
import ru.sonarplus.kernel.sqlobject.objects.CommonTableExpression;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class CTEDistiller extends CommonBaseDistiller {

	public CTEDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof CommonTableExpression;
	}
	
	protected void checkCTE(Select select){
		CTEsContainer with = select.getWith();
		Preconditions.checkArgument( !((with != null) && with.isHasChilds()),
				"Используемые внутри конструкции WITH подзапросы не могут содержать WITH");
	}
	
	protected void iteratecheckCTE(SqlObject root)
			throws SqlObjectException {
		if (root instanceof Select) {
			checkCTE((Select) root);
		}
		for (SqlObject item: root) {
			iteratecheckCTE(item);
		}
		
	}
	
	protected void checkCTEInSubQuery(Select select) {
		
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException {
		SqlObject result = state.sqlObject;
		checkCTEInSubQuery(((CommonTableExpression) result).getSelect());
		return result;
	}

}
