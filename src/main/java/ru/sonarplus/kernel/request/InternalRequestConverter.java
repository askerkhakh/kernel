package ru.sonarplus.kernel.request;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sonarplus.kernel.OracleQueryParameter;
import ru.sonarplus.kernel.column_info.ColumnInfo;
import ru.sonarplus.kernel.column_info.ColumnInfoCollector;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.request.param_converter.ParamConverterDefault;
import ru.sonarplus.kernel.request.param_converter.ParamConverterException;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertorDebug;
import ru.sonarplus.kernel.sqlobject.distillation.Distiller;
import ru.sonarplus.kernel.sqlobject.distillation.DistillerParams;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.objects.CursorSpecification;
import ru.sonarplus.kernel.sqlobject.objects.ParamRef;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.QueryParams;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.ParamsClauseBuilder;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class InternalRequestConverter {

    public final Logger LOG;
    public final SqlObjectsConvertor convertor;
    private final ParamConverterDefault parameterConverter;

    public static class RequestStatementContainer implements StatementContainer {

        private static final Logger LOG = LoggerFactory.getLogger(RequestStatementContainer.class);

        private final String sql;
        private final OracleQueryParameter[] sortedParams;
        private final ColumnInfo[] columnsInfo;

        public RequestStatementContainer(String sql, OracleQueryParameter[] originalParams, ColumnInfo[] columnsInfo) {
            this.sql = sql;
            this.sortedParams = setParamIndexes(originalParams);
            if (LOG.isDebugEnabled()) {
                LOG.debug("sorted params: {}", Arrays.toString(sortedParams));
            }
            this.columnsInfo = columnsInfo;
        }

        private OracleQueryParameter[] setParamIndexes(OracleQueryParameter... originalParams) {
            OracleQueryParameter[] params = originalParams;
            if (originalParams == null) {
                params = new OracleQueryParameter[0];
            }
            for (int i = 0; i < params.length; i++) {
                params[i].setIndex(i + 1);
            }
            return params;
        }

        @Override
        public String getSql() {
            return sql;
        }

        @Override
        public OracleQueryParameter[] getParamsArray() {
            return sortedParams;
        }

        @Override
        public ColumnInfo[] getColumnsInfo() {
            return columnsInfo;
        }
    }

    @Inject
    public InternalRequestConverter(SqlObjectsConvertor convertor, ParamConverterDefault parameterConverter) {
       this.LOG = LoggerFactory.getLogger(this.getClass());
       this.convertor = convertor;
       this.parameterConverter = parameterConverter;
    }

    public StatementContainer convert(String sessionId, DbSchemaSpec dbSchemaSpec, RequestData requestData)
            throws Exception {
        LOG.debug("converting request: {}", requestData.getRequest());
        return internalConvert(dbSchemaSpec, buildRequest(sessionId, requestData.getRequest()), requestData.getAttributes(), null);
    }

    public StatementContainer convert(String sessionId, DbSchemaSpec dbSchemaSpec, RequestData requestData, String jsonParams)
            throws Exception {
        LOG.debug("converting request: {}", requestData.getRequest());
        return internalConvert(dbSchemaSpec, buildRequest(sessionId, requestData.getRequest()), requestData.getAttributes(), buildQueryParams(sessionId, jsonParams));
    }

    public StatementContainer convert(String sessionId, DbSchemaSpec dbSchemaSpec, RequestData requestData, QueryParams params)
            throws Exception {
        LOG.debug("converting request: {}", requestData.getRequest());
        return internalConvert(dbSchemaSpec, buildRequest(sessionId, requestData.getRequest()), requestData.getAttributes(), params);
    }

    public StatementContainer convert(String sessionId, DbSchemaSpec dbSchemaSpec, RequestData requestData, OracleQueryParameter... params)
            throws Exception {
        LOG.debug("converting request: {}", requestData.getRequest());
        return internalConvert(dbSchemaSpec, buildRequest(sessionId, requestData.getRequest()), requestData.getAttributes(), buildQueryParams(params));
    }

    protected abstract SqlObject buildRequest(String sessionId, String request) throws Exception;

    protected StatementContainer internalConvert(DbSchemaSpec dbSchemaSpec, SqlObject request, RequestData.Attributes requestAttributes, QueryParams queryParams)
            throws Exception {
        LOG.debug("sqlobject request: {}", SqlObjectsConvertorDebug.dc(request));
        SqlObject preparedRequest = prepareRequest(dbSchemaSpec, request, requestAttributes, queryParams);
        ColumnInfo[] columnsInfo = getColumnsInfo(preparedRequest, dbSchemaSpec);
        OracleQueryParameter[] oraParams = buildSqlRequestParams(this.parameterConverter, preparedRequest);
        String sql = Preconditions.checkNotNull(convertor).convert(preparedRequest);
        LOG.debug("sql request: {}", sql);
        return new RequestStatementContainer(sql, oraParams, columnsInfo);
    }

    protected SqlObject prepareRequest(DbSchemaSpec dbSchemaSpec, SqlObject request, RequestData.Attributes requestAttributes, QueryParams queryParams)
            throws Exception{
        if (!(request instanceof SqlQuery))
            return request;
        SqlQuery sqlQuery = (SqlQuery) request;
        // установили именованные параметры, переданные отдельно
        if (queryParams != null)
            sqlQuery.setParams(queryParams);

        // в полученном sqlObject-запросе могут быть (наверное) не поднятые "технические", безымянные параметры,
        // определённые в теле запроса. нужно их "поднять" в раздел
        ParamsClauseBuilder.buildParamsClause(sqlQuery);
        Distiller.distillate(
                sqlQuery,
                new DistillerParams(
                    dbSchemaSpec,
                    convertor.getDBSupport(),
                    requestAttributes.useNulls(),
                    // значение даты-времени для использования в sql-выражениях с текущей датой/временем.
                    // если NULL - в выражениях будут использованы соответствующие функции БД (Oracle: SYSDATE)
                    // в противном случае будут создаваться параметризованные выражения
                    requestAttributes.workStationLocalDateTime()
                )
        );

        if (sqlQuery.getClass() == Select.class)
            return ((Select) sqlQuery).toCursorSpecification(false);

        return sqlQuery;
    }

    protected ColumnInfo[] getColumnsInfo(SqlObject request, DbSchemaSpec dbSchemaSpec)
            throws Exception{
        if (request.getClass() == CursorSpecification.class)
            return ColumnInfoCollector.collect((CursorSpecification) request, dbSchemaSpec, this.convertor);
        return null;
    }

    protected QueryParams buildQueryParams(String sessionId, String jsonParams)
            throws Exception{
        if (!StringUtils.isEmpty(jsonParams))
            return (QueryParams) JsonSqlParser.parseJsonString(jsonParams, sessionId);
        return null;
    }

    @Nullable
    public static OracleQueryParameter[] buildSqlRequestParams(ParamConverterDefault parameterConverter, SqlObject request) {
        if (!(request instanceof SqlQuery))
            return null;

        QueryParams queryParams = ((SqlQuery) request).getParams();
        if (queryParams == null || queryParams.itemsCount() == 0)
            return null;

        // извлечём из запроса имена параметров, в порядке их использования
        List<String> paramNames = extractParamNamesInOrderOfUsing(request);

        OracleQueryParameter[] oraParams = new OracleQueryParameter[paramNames.size()];
        // параметры для Oracle создаются с простыми именами PNN
        for (int i = 0; i < paramNames.size(); i++)
            // TODO вместо того, чтобы повторно конвертировать одноимённые QueryParam'ы в OracleQueryParameter'ы можно кешировать...
            oraParams[i] = buildOraParam(parameterConverter, i, queryParams.findExistingParam(paramNames.get(i)));
        // нет необходимости пробегаться по запросу, переименовывая параметры,
        // т.к. при конвертации в sql все ссылки на параметры будут представлены символом '?'
        return oraParams;
    }
    protected QueryParams buildQueryParams(OracleQueryParameter... params) {
        throw new NotImplementedException(StatementContainer.class);
    }

    protected static void recursiveExtractParamNamesInOrderOfUsing(SqlObject root, List<String> paramNames) {
        if (root.getClass() == QueryParams.class)
            return;
        if (root.getClass() == ParamRef.class) {
            paramNames.add(((ParamRef)root).parameterName);
            return;
        }
        for (SqlObject item: root)
            recursiveExtractParamNamesInOrderOfUsing(item, paramNames);
    }

    protected static List<String> extractParamNamesInOrderOfUsing(SqlObject root) {
        List<String> paramNames = new ArrayList<>();
        recursiveExtractParamNamesInOrderOfUsing(root, paramNames);
        return paramNames;
    }

    protected static OracleQueryParameter buildOraParam(ParamConverterDefault parameterConverter, int index, QueryParam param)
            throws ParamConverterException {
        OracleQueryParameter oraParam = new OracleQueryParameter();
        oraParam.setIndex(index);
        // параметры позиционные, поэтому имя простое: PNN
        oraParam.setName("P" + String.valueOf(index));
        oraParam.setDirection(parameterConverter.paramTypeToOraParamDirection(param.getParamType()));
        oraParam.setType(parameterConverter.fieldTypeIdToJDBCTypes(param.getValueType()));
        oraParam.setValue(parameterConverter.paramValueToJavaValue(param.getValue(), param.getValueType()));
        return oraParam;
    }

}
