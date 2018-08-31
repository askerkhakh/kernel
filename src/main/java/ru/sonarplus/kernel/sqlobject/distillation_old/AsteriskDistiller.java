package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedField;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class AsteriskDistiller extends CommonBaseDistiller {

	public AsteriskDistiller() {

	}

	@Override
	public boolean isMy(SqlObject source) {
		return (source instanceof QualifiedField) && SqlObjectUtils.isAsterisk((QualifiedField) source);
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		AsteriskSupport.buildAsteriskColumns((QualifiedField) state.sqlObject, state.namesResolver);
		return null;
	}

}
