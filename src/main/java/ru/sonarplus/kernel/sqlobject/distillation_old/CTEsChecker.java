package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.objects.CTEsContainer;
import ru.sonarplus.kernel.sqlobject.objects.CommonTableExpression;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CTEsChecker {
	protected Map<String, CommonTableExpression> aliasesCTEs = new HashMap<String, CommonTableExpression>();

	public CTEsChecker() {
	}
	
	
	protected String getNameForMessage(CommonTableExpression cte) {
		return cte.alias + "("+ String.join(",",cte.columns) + ")";
	}
	
	public void execute(CTEsContainer ctes) {
		Set<String> duplicates = new HashSet<String>();
		for (SqlObject item: ctes) {
			CommonTableExpression cte = (CommonTableExpression) item;
			CommonTableExpression prevCte = aliasesCTEs.put(cte.alias, cte);
			if (prevCte != null) {
				aliasesCTEs.put(prevCte.alias, prevCte);
				duplicates.add(getNameForMessage(prevCte));
				duplicates.add(getNameForMessage(cte));
			}
		}
		if (duplicates.isEmpty()) {
			return;
		}
		String msgError = "В запросе найдены дублирующиеся CTE-подзапросы: \n"+String.join("\n", duplicates);
		Preconditions.checkState(false, msgError);
	}

}
