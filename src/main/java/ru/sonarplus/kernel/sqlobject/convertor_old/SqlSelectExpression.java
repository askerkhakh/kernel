package ru.sonarplus.kernel.sqlobject.convertor_old;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.objects.Join;

/* структура для передачи в процедуры конвертации всех строк sql-выражения.
    JoinedAlias, JoinType - для передачи "контекста", в котором будут строиться
    условия сравнения (условие для подлитых таблиц или нет).
    Необходимость использования возникнет, в частности, при построении
    слияния таблиц в нотации Oracle. */
  
public class SqlSelectExpression {
	public String with = "";
	public String columns = "";
	public String from = "";
	public String where = "";
	public String groupBy = "";
	public String joinedAlias = "";
	public Join.JoinType joinType = Join.JoinType.INNER;
	public String unions = "";
	
	public String asString(boolean distinct, String hint) {
		return with + 
				"SELECT " + (!StringUtils.isEmpty(hint) ? hint : "") +
				(distinct ? "DISTINCT " : "")+
				columns +
				(!StringUtils.isEmpty(from) ? " FROM "+from : "")+
				(!StringUtils.isEmpty(where) ? " WHERE "+ where: "") +
				(!StringUtils.isEmpty(groupBy) ? " GROUP BY "+ groupBy: "") +
				unions;
	}

	public SqlSelectExpression() {

	}

}
