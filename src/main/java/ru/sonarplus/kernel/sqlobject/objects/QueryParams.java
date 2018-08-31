package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class QueryParams extends SqlObjectsArray {

	public QueryParams() { super(); }

	public QueryParams(SqlObject owner) throws SqlObjectException {
		super(owner);
	}

	@Override
    protected Class getItemClass() { return QueryParam.class; }

	public QueryParam findParam(String name) {
		for (SqlObject item: this){
			if (item.getClass() == QueryParam.class) {
				QueryParam param = (QueryParam) item;
				if ((param.parameterName != null) &&
					(param.parameterName.equals(name))) {
					return param;
				}
			} 
		}
		return null;
	}
	
	public QueryParam findExistingParam(String name) {
		QueryParam param = findParam(name);
		Preconditions.checkNotNull(param);
		return param;
	}
	
	public QueryParam addQueryParam(QueryParam param)
            throws SqlObjectException {
		Preconditions.checkNotNull(param);
		QueryParam oldParam = findParam(param.parameterName);
		if (oldParam == null) {
			Preconditions.checkState(
					(param.parameterName != null)
					&& (!StringUtils.isEmpty(param.parameterName)));
			insertItem(param);
		}
		else {
			Preconditions.checkState(oldParam == param,
					"Дублирование имён параметров запроса (%s)",
					param.parameterName);
		}
		return param;
	}
	
	public FieldTypeId getValueType(String name) { return findExistingParam(name).getValueType(); }

}
