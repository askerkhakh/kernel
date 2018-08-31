package ru.sonarplus.kernel;

import ru.sonarplus.kernel.column_info.ColumnInfoCollector;
import ru.sonarplus.kernel.field_value_converter.FieldValueConverter;
import ru.sonarplus.kernel.recordset.RecordSet;
import ru.sonarplus.kernel.recordset.RecordSetFactory;
import ru.sonarplus.kernel.request.InternalRequestConverter;
import ru.sonarplus.kernel.request.param_converter.ParamConverterDefault;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.distillation.Distiller;
import ru.sonarplus.kernel.sqlobject.distillation.DistillerParams;
import ru.sonarplus.kernel.sqlobject.objects.CursorSpecification;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.ParamsClauseBuilder;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * SqlObjectExecutionServiceImpl
 */
@Named
@Singleton
public class SqlObjectExecutionServiceImpl implements SqlObjectExecutionService {

    private final SqlObjectsConvertor converter;
    private final ParamConverterDefault parameterConverter;
    private final FieldValueConverter fieldValueConverter;

    @Inject
    public SqlObjectExecutionServiceImpl(SqlObjectsConvertor converter, ParamConverterDefault parameterConverter,
                                         FieldValueConverter fieldValueConverter) {
        this.parameterConverter = parameterConverter;
        this.converter = converter;
        this.fieldValueConverter = fieldValueConverter;
    }

    public int executeCommand(ClientSession session, SqlObject command) throws Exception {
        try (PreparedStatement preparedStatement = prepareStatement(session, command)) {
            return preparedStatement.executeUpdate();
        }
    }

    @Override
    public RecordSet executeCursor(ClientSession session, CursorSpecification cursorSpecification) throws Exception {
        ResultSet resultSet;
        try (PreparedStatement preparedStatement = prepareStatement(session, cursorSpecification)) {
            resultSet = preparedStatement.executeQuery();
        }
        return RecordSetFactory.newInstance(
                resultSet,
                ColumnInfoCollector.collect(cursorSpecification, session.getDbSchemaSpec(), converter),
                fieldValueConverter
        );
    }

    private PreparedStatement prepareStatement(ClientSession session, SqlObject sqlObject) throws Exception {
        // "подняли" параметры, если они есть
        if (sqlObject instanceof SqlQuery) {
            ParamsClauseBuilder.buildParamsClause((SqlQuery) sqlObject);
        }
        // продистиллировали запрос - получили СУБД-зависимый sqlobject
        Distiller.distillate(
                sqlObject,
                new DistillerParams(
                        session.getDbSchemaSpec(),
                        converter.getDBSupport(),
                        converter.getUseStandardNulls()
                )
        );
        // конвертируем СУБД-зависимый sqlobject-запрос в СУБД-зависимую строку, а sqlobject-параметры (если они есть)
        // в СУБД-зависимые параметры
        String sql = converter.convert(sqlObject);
        OracleQueryParameter[] params = InternalRequestConverter.buildSqlRequestParams(parameterConverter, sqlObject);

        PreparedStatement statement = session.getConnection().prepareStatement(sql);
        if (params != null) {
            for (OracleQueryParameter param : params) {
                // в statement параметры с 1
                statement.setObject(param.getIndex() + 1, param.getValue());
            }
        }
        return statement;
    }

}