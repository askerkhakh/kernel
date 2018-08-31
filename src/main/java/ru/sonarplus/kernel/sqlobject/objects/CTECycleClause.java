package ru.sonarplus.kernel.sqlobject.objects;

//     раздел CYCLE подзапроса в разделе WITH

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class CTECycleClause extends SqlObject {
	public String markerCycleColumn;
	public String markerCycleValue;
	public String markerCycleValueDefault;
	public String[] columns;
	public String columnPath;

    public CTECycleClause() { super(); }

	public CTECycleClause(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
	
	public CTECycleClause setColumns(String... columns) {
		this.columns = columns;
		return this;
	}
	
	public CTECycleClause defaultValue(String value) {
		markerCycleValueDefault = value;
		return this;
	}
	
	public CTECycleClause using(String columnPath) {
		this.columnPath = columnPath;
		return this;
	}
	
	public CTECycleClause set(String markerCycleColumn) {
		this.markerCycleColumn = markerCycleColumn;
		return this;
	}

	public CTECycleClause to(String markerCycleValue) {
		this.markerCycleValue = markerCycleValue;
		return this;
	}

}
