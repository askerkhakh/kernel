package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.objects.OraFTSMarker;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class FTSOraMarkerDistiller extends CommonBaseDistiller {

	public FTSOraMarkerDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof OraFTSMarker;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException {
		state.root.techInfo.addOraFtsMarker(state.sqlObject);
		return state.sqlObject;
	}
	

}
