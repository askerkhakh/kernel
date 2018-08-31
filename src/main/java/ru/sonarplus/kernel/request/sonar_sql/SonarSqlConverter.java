package ru.sonarplus.kernel.request.sonar_sql;

import ru.sonarplus.kernel.request.InternalRequestConverter;
import ru.sonarplus.kernel.request.param_converter.ParamConverterDefault;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.sql_parse.SqlLexer;
import ru.sonarplus.kernel.sqlobject.sql_parse.SqlParser;

import java.io.StringReader;

public class SonarSqlConverter extends InternalRequestConverter {

    public SonarSqlConverter(SqlObjectsConvertor convertor, ParamConverterDefault parameterConverter) {
        super(convertor, parameterConverter);
    }

    @Override
    protected SqlObject buildRequest(String sessionId, String request)
            throws Exception{
        SqlParser p = new SqlParser(new SqlLexer(new StringReader(request)));
        return (SqlObject) p.parse().value;
    }
}
