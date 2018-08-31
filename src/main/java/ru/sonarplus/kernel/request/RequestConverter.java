package ru.sonarplus.kernel.request;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.OracleQueryParameter;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.request.json.JsonConverter;
import ru.sonarplus.kernel.request.param_converter.ParamConverterDefault;
import ru.sonarplus.kernel.request.sonar_sql.SonarSqlConverter;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.objects.QueryParams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Named
@Singleton
public class RequestConverter implements StatementConverter {

    private final SqlObjectsConvertor convertor;
    private final ParamConverterDefault parameterConverter;
    private JsonConverter jsonConverter = null;
    private SonarSqlConverter sqlConverter = null;

    public static class RequestConverterException extends Exception {
        public RequestConverterException(String msg) { super(msg);}
    }

    public static class RequestParamsContainerJson extends StatementConverter.RequestParamsContainer {
        public final String params;
        public RequestParamsContainerJson(String params) { this.params = params; }

        @Override
        public String toString() {
            return super.toString()  + ' ' + String.valueOf(params);
        }
    }

    public static class RequestParamsContainerQueryParams extends StatementConverter.RequestParamsContainer {
        public final QueryParams params;
        public RequestParamsContainerQueryParams(QueryParams params) { this.params = params; }
    }

    public static class RequestParamsContainerOracleQueryParameters extends StatementConverter.RequestParamsContainer {
        public final OracleQueryParameter[] params;
        public RequestParamsContainerOracleQueryParameters(OracleQueryParameter... params) { this.params = params; }
    }

    @Inject
    public RequestConverter(SqlObjectsConvertor convertor, ParamConverterDefault parameterConverter) {
        this.convertor = convertor;
        this.parameterConverter = parameterConverter;
    }

    @Override
    public StatementContainer convert(String sessionId, DbSchemaSpec dbSchemaSpec, String request, RequestParamsContainer params)
            throws Exception {
        if (params == null)
            return getConverter(request).convert(sessionId, dbSchemaSpec, RequestData.parse(request));
        else if (params.getClass() == RequestParamsContainerJson.class)
            return getConverter(request).convert(sessionId, dbSchemaSpec, RequestData.parse(request), ((RequestParamsContainerJson) params).params);
        else if (params.getClass() == RequestParamsContainerQueryParams.class)
            return getConverter(request).convert(sessionId, dbSchemaSpec, RequestData.parse(request), ((RequestParamsContainerQueryParams) params).params);
        else if (params.getClass() == RequestParamsContainerOracleQueryParameters.class)
            return getConverter(request).convert(sessionId, dbSchemaSpec, RequestData.parse(request), ((RequestParamsContainerOracleQueryParameters) params).params);
        else
            throw new RequestConverterException(String.format("Контейнер параметров '%s' не поддержан", params.getClass().getSimpleName()));
    }

    protected InternalRequestConverter getConverter(String request) {
        if (isJsonRequest(request))
            return getJsonConverter();
        else
            return getSqlConverter();
    }

    protected boolean isJsonRequest(String request) { return !StringUtils.isEmpty(request) && request.charAt(0) == '{'; }

    protected JsonConverter getJsonConverter() {
        if (this.jsonConverter == null)
            this.jsonConverter = new JsonConverter(convertor, parameterConverter);
        return this.jsonConverter;
    }

    protected SonarSqlConverter getSqlConverter() {
        if (this.sqlConverter == null)
            this.sqlConverter = new SonarSqlConverter(convertor, parameterConverter);
        return this.sqlConverter;
    }

}
