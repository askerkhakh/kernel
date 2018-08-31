package ru.sonarplus.kernel.request.json;

import ru.sonarplus.kernel.request.InternalRequestConverter;
import ru.sonarplus.kernel.request.param_converter.ParamConverterDefault;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class JsonConverter extends InternalRequestConverter {

	public JsonConverter(SqlObjectsConvertor convertor, ParamConverterDefault parameterConverter) {
		super(convertor, parameterConverter);
	}

	@Override
    protected SqlObject buildRequest(String sessionId, String request) throws Exception {
        return JsonSqlParser.parseJsonString(request, sessionId);
    }
}

/*
15.03.2016 12:08 И.В.Ковалев  
  * Код задачи 40220. При конвертации json в sql добавлена установка параметра, 
	отвечающего за добавление кавычек в алиасы колонок в контекст генерации sql.
28.12.2017 19:13 А.Б.Хах
 P* Код задачи 44149. JsonConverter теперь ожидает недистиллированный запрос в виже json, 
 	выполняет построение sqlobjects по json, дистиллирует sqlobjects и конвертирует в sql.
*/