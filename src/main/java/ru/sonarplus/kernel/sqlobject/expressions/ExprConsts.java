package ru.sonarplus.kernel.sqlobject.expressions;

public final class ExprConsts {
	public final static String MARKER = "#";
	
	public final static String EXPR_BEGIN = MARKER + 
			"B2EX505KE" + MARKER;

	public final static String EXPR_END = 
			MARKER + "BRXPHD63Y" + MARKER;
	
	// разделитель тега и содержимого выражения: 'expr_qname_sonar#1[ALIAS].Name'
	public final static String DELIMITER = MARKER + "ACW4SN50U" + MARKER;
	// разделитель аргументов функции
	public final static String ARG_DELIMITER = MARKER + "ACW19UGBL" + MARKER;

	public final static String QNAME = "EXPR_QNAME_SONAR";         // квалифицированное поле
	public final static String QRNAME = "EXPR_QRNAME_SONAR";       // квалифицированное русскоязычное поле
	
	// замена имени поля на вызов функции, возвращающей его текстовое значение, в случае, если поле бинарное
	public final static String FUNCTION_BINDATA_TOTEXT = "expr_bindata_to_text_sonar";  
	public final static String FUNCTION_UPPER = "expr_upper_sonar"; // функция Upper
	public final static String FUNCTION_COUNT = "expr_count_sonar"; // ...Count
	public final static String FUNCTION_MAX = "expr_max_sonar";     // ...Max
	public final static String FUNCTION_MIN = "expr_min_sonar";     // ...Min
	public final static String FUNCTION_SUM = "expr_sum_sonar";     // ...Sum
	public final static String FUNCTION_AVG = "expr_avg_sonar";     // ...Avg
	public final static String FUNCTION_COALESCE = "expr_coalesce_sonar"; // Coalesce (более общий вариант, чем NVL(), IFNULL())
	public final static String FUNCTION_ROUND = "expr_round_sonar"; // ...Round(float, digits)
	
	// 'Год из даты' - функция для сопоставления с внешними данными
	public final static String FUNCTION_YEAR_FROM_DATE = "expr_year_from_date"; 

	
	public ExprConsts() {
	}

}
