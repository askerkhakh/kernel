package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Set;

public class PredicateIsNullDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);

	public PredicateIsNullDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateIsNull;
	}
	
	protected void setTechInfos(SqlObject root, FieldTypeId type) {
		if (!(root instanceof ColumnExpression)) {
			for (SqlObject item: root) {
				setTechInfos(item , type);
			}
		}
		else if (! Utils.isTechInfoFilled((ColumnExpression) root)) {
			SqlObjectUtils.buildTechInfoForExpr((ColumnExpression) root, type, true);
		}
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateIsNull predicate = (PredicateIsNull) state.sqlObject;
		ColumnExpression expr = predicate.getExpr();
		Preconditions.checkNotNull(expr, "Для условия %s не задан аргумент", predicate.getClass().getSimpleName());
		if (!Utils.isTechInfoFilled(checkNotRecordId(expr, state))) {
			internalDistillateObject(new DistillerState(expr, state), COLUMN_EXPRESSION, null);
		}
		if (Utils.isTechInfoFilled(expr)) {
			ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(expr);
			predicate.isRaw = predicate.isRaw || state.dbc.useStandardNulls;
			if ((!predicate.isRaw) && (expr instanceof QualifiedField) && (techInfo.fieldTypeId != FieldTypeId.tid_UNKNOWN)) {
		          // для дистиллированного поля создаём условие сравнения с нашим 0-вым значением или набор условий (для blob/clob)
				QualifiedField qField = (QualifiedField) expr;
				QualifiedName qName = new QualifiedName(fieldAliasPart(qField), techInfo.nativeFieldName);
				FieldTypeId valueType = techInfo.fieldTypeId;
				Predicate predicateNew = state.dbc.dbSupport.nullComparisonTranslate(qName, valueType, predicate.not);
	            /* Формально назначим описатели в созданном условии выражениям (значениям)
	                в противном случае, запрос, содержащий данное условие будет определяться
	                как недистиллированный.
	                Подразумевается, что в результате nullComparison_Translate не будет
	                объектов QualifiedRField
	              */
				
				setTechInfos(predicateNew, valueType);
				replace(predicate, predicateNew);
			}
		}
		else {
			addNotDistillatedObject(predicate, state);
		}
		return predicate;
	}

}
