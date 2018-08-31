package ru.sonarplus.kernel.request;

import ru.sonarplus.kernel.OracleQueryParameter;
import ru.sonarplus.kernel.column_info.ColumnInfo;

public interface StatementContainer {

	String getSql();

	OracleQueryParameter[] getParamsArray();

	ColumnInfo[] getColumnsInfo();

}