package ru.sonarplus.kernel.sqlobject.objects;


import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class Parameter extends ColumnExpression {
	public String parameterName;

	// соответствует заполняемому в Delphi полю TParamRef.FDynamicInfo
	public String dynamicInfo = null;

	public Parameter() { super(); }

	public Parameter(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
	
    public boolean isContainedInParamsClause() {
    	return owner instanceof QueryParams;
    }
	

}
