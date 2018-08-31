package ru.sonarplus.kernel.request;

import ru.sonarplus.kernel.dbschema.DbSchemaSpec;

/**
 * 
 * @author gubanov
 * 
 */
public interface StatementConverter {

	class RequestParamsContainer {}

	/**
	 * Преобразует запрос + переданные параметры в объект StatementContainer
	 * 
	 * @param dbSchemaSpec описатель
	 * @param request запрос от клиента
	 * @param params параметры запроса
	 * @return StatementContainer, содержащий SQL (полученный из запроса) и массив параметров идущих в порядке
	 *         использования их в SQL
	 */
	StatementContainer convert(String sessionId, DbSchemaSpec dbSchemaSpec, String request, RequestParamsContainer params) throws Exception;

}

/*
15.03.2016 12:08 И.В.Ковалев  
  * Код задачи 40220. При конвертации json в sql добавлен параметр, 
    отвечающий за добавление кавычек в алиасы колонок.    
*/