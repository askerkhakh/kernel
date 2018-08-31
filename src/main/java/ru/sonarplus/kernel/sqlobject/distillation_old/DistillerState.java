package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.List;
import java.util.Set;

public class DistillerState implements Cloneable{
	public DbcRec dbc;
	public NamesResolver namesResolver;
	public SqlObject sqlObject;
	public SqlQuery root;
	public DistillationParamsIn paramsIn;
	public Set<SqlObject> objectsNotDistilated;
	public List<SqlObject> paramsForWrapWithExpr; // перечень параметров в теле запроса для 'оборачивания' их в СУБД-зависимое выражение

	public DistillerState(SqlObject sqlObject,
			DbcRec dbc, NamesResolver namesResolver,
			DistillationParamsIn paramsIn,
			Set<SqlObject> objectsNotDistilated,
			List<SqlObject> paramsForWrapWithExpr) {
		this.sqlObject = sqlObject;
		this.dbc = dbc;
        // #BAD# как и в DSP - устанавливаем корневой запрос безусловно получая его от дистиллируемого объекта.
        // по-видимому это неверно (и в DSP тоже), т.к. есть прецеденты, когда при дистилляции (полнотекстового условия, например)
        // создаются объекты, снова подлежащие дистилляции.
        // т.к. эти объекты могут быть не включены в дерево запроса - получение у них корневого запроса будет неверным,
        // в результате не будет доступа к разделу параметров корневого запроса
        // TODO Скорее всего это root должен выставляться один раз при начале дистилляции корня,
        // и каждый DistillerState должен создаваться с использованием уже существующего root'а
        this.root = SqlObjectUtils.getRootQuery(sqlObject);
		this.namesResolver = namesResolver;
		this.paramsIn = paramsIn;
		this.objectsNotDistilated = objectsNotDistilated;
		this.paramsForWrapWithExpr = paramsForWrapWithExpr;
	}
	
	public DistillerState(SqlObject sqlObject, DbcRec dbc) {
		this(sqlObject, dbc, null, new DistillationParamsIn(FieldTypeId.tid_UNKNOWN),
				null, null);
	}
	
	public DistillerState(SqlObject sqlObject, DistillerState state) {
		this(sqlObject, state.dbc, state.namesResolver, state.paramsIn, state.objectsNotDistilated,
				state.paramsForWrapWithExpr);
	}
	
	public DistillerState(SqlObject sqlObject,
			DistillationParamsIn paramsIn,
			DistillerState state) {
		this(sqlObject, state.dbc, state.namesResolver, paramsIn, state.objectsNotDistilated,
				state.paramsForWrapWithExpr);
	}
	
	public DistillerState clone() throws CloneNotSupportedException {
		return (DistillerState) super.clone();
	}	
	

}
