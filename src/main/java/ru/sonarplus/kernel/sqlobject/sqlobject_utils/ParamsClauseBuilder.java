package ru.sonarplus.kernel.sqlobject.sqlobject_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.*;

public class ParamsClauseBuilder {

    public static class ParamsClauseBuilderException extends RuntimeException{

        public ParamsClauseBuilderException(String message) {
            super(message);
        }
    }

    public static void buildParamsClause(SqlQuery request) {
        request.setParams(createParamsClause(request));
    }

    public static QueryParams createParamsClause(SqlQuery request) {
        BuildParamsClauseContext context = new BuildParamsClauseContext();
        walkRequest(request, context);
        replaceParamsWithParamRefs(context);
        prepareTechParams(context);
        deleteParamsClauses(context);

        QueryParams params = new QueryParams();
        ParamValueDescr paramValue;
        for (String paramName: context.paramNames) {
            paramValue = Preconditions.checkNotNull(context.paramValues.get(paramName));
            params.insertItem(paramValue.buildQueryParam());
        }
        return params;
    }

    protected static void prepareTechParams(BuildParamsClauseContext context) {
        for (Parameter param: context.techParams) {
            // "техническому" параметру назначим имя
            param.parameterName = context.getNewTechParamName();
            // вынесем параметр в раздел запроса
            updateParamDescr(param, null, context);
            context.paramNames.add(param.parameterName);
            // в теле запроса оставим ссылку на параметр
            param.getOwner().replace(param, new ParamRef(param.parameterName));
        }
    }

    protected static void replaceParamsWithParamRefs(BuildParamsClauseContext context)
            throws SqlObjectException {
        for (Parameter param: context.paramsForReplaceWithParamRef)
            param.getOwner().replace(param, new ParamRef(param.parameterName));
    }

    protected static void deleteParamsClauses(BuildParamsClauseContext context)
            throws SqlObjectException {
        for (QueryParams params: context.queryParamClausesForDelete)
            params.getOwner().removeItem(params);
    }

    protected static class BuildParamsClauseContext {
        public Map<String, ParamValueDescr> paramValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        public Set<String> paramNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        public Set<Parameter> techParams = new HashSet<>();
        public List<QueryParams> queryParamClausesForDelete = new ArrayList<>();
        public List<Parameter> paramsForReplaceWithParamRef = new ArrayList<>();

        private int techParamCount = 0;
        public String getNewTechParamName() {
            String prefix = SqlObjectUtils.getTechParamPrefix(null);
            while (paramNames.contains(prefix + Integer.toString(techParamCount)))
                techParamCount++;
            return prefix + Integer.toString(techParamCount);
        }
    }

    protected static void walkRequest(SqlObject item, BuildParamsClauseContext context) {
        QueryParams params = null;
        // если это запрос, содержащий раздел параметров - извлечём сначала объявленные параметры
        // в порядке их добавления
        if (item instanceof SqlQuery) {
            params = ((SqlQuery) item).getParams();
            if (params != null)
                walkRequest(params, context);
        }
        for (SqlObject child: item)
            if (child != params)
                walkRequest(child, context);
        updateParamDescrFrom(item, context);
    }

    protected static void updateParamDescrFrom(SqlObject item, BuildParamsClauseContext context) {
        if (item instanceof QueryParams)
            // все, имеющиеся в запросе разделы параметров (в запросе и подзапросах)
            // соберём линейный список с тем, чтобы их потом убрать из запроса
            context.queryParamClausesForDelete.add((QueryParams) item);
        else if (item instanceof Expression)
            // отработаем имеющиеся в строке ссылки на параметры
            for (String paramRef: ExprUtils.getParamRefs(((Expression) item).getExpr()))
                updateParamDescr(null, paramRef, context);
        else if (item instanceof Parameter) {
            Parameter parameter = (Parameter) item;
            if ((parameter.isContainedInParamsClause()) || (parameter instanceof ParamRef))
                // параметр в разделе параметров или ссылка в теле запроса на параметр обязаны иметь непустое имя
                Preconditions.checkState(!StringUtils.isEmpty(parameter.parameterName));

            if (SqlObjectUtils.isTechParam(parameter))
                context.techParams.add(parameter);
            else if (parameter.isContainedInParamsClause())
                updateParamDescr(parameter, null, context);
            else if (parameter instanceof QueryParam) {
                updateParamDescr(parameter, null, context);
                // исходный параметр в теле запроса заменим ссылкой на него
                context.paramsForReplaceWithParamRef.add(parameter);
            }
            else if (parameter instanceof ParamRef)
                updateParamDescr(null, parameter.parameterName, context);
        }
    }

    protected static void updateParamDescr(Parameter param, String paramName, BuildParamsClauseContext context) {
        String lParamName;
        if (param != null)
            lParamName = Preconditions.checkNotNull(param.parameterName).toUpperCase().trim();
        else
            lParamName = Preconditions.checkNotNull(paramName).toUpperCase().trim();
        Preconditions.checkArgument(!StringUtils.isEmpty(lParamName));
        ParamValueDescr paramValueDescr = context.paramValues.get(lParamName);
        if (paramValueDescr == null) {
            if (param != null)
                context.paramValues.put(lParamName, new ParamValueDescr(param));
            else
                context.paramValues.put(lParamName, new ParamValueDescr(lParamName));
        }
        else if (param != null)
            paramValueDescr.updateFrom(param);
        context.paramNames.add(lParamName);
    }

    protected static class ParamValueDescr {
        private static final String UNSUPPORTED_PARAM_CLASS_TYPE = "В процедуре формирования раздела параметров не поддержаны параметры \"%s:%s\"";
        private static final String INCOMPATIBLE_RECID = "Несовместимые recid-параметры (\"%s\")";
        private static final String INCOMPATIBLE_VALUES_RECID_AND_CONST = "Несовместимые значения (recid и константа) для параметра \"%s\"";
        private static final String VARIOUS_VALUES_FOR_PARAMETER = "Различные значения параметра \"%s\"";
        private static final String PARAM_TYPES_CONFLICT = "Конфликт типов (%s - %s) одноимённых (%s) параметров";
        public String name;
        private QueryParam parameter;

        public ParamValueDescr(String name) {
            Preconditions.checkArgument(!StringUtils.isEmpty(name));
            this.name = name;
        }

        public ParamValueDescr(Parameter param) {
            Preconditions.checkArgument(!StringUtils.isEmpty(Preconditions.checkNotNull((param).parameterName)));
            this.parameter = (QueryParam) param;
            this.name = param.parameterName;
        }

        public void updateFrom(Parameter parameter) {
            Preconditions.checkNotNull(parameter);
            if (!(parameter instanceof ParamRef) && parameter.getClass() == QueryParam.class) {
                if (this.parameter == null)
                    this.parameter = (QueryParam) parameter.getClone();
                else {
                    checkCompatibilityWith((QueryParam) parameter);
                    replaceValueIfNeedFrom((QueryParam) parameter);
                }
            }
            else
                throw new ParamsClauseBuilderException(String.format(UNSUPPORTED_PARAM_CLASS_TYPE, parameter.getClass().getName(), parameter.parameterName));
        }

        public QueryParam buildQueryParam() {
            QueryParam param = this.parameter;
            this.parameter = null;
            // если this.parameter = null, это значит, что какой-то из имеющихся в запросе ParamRef'ов
            // ссылается на отсутствующий параметр
            // хотя TODO может быть здесь нужно создавать параметр, содержащий NULL/tid_UNKNOWN
            return Preconditions.checkNotNull(param);
        }

        protected void checkCompatibilityWith(QueryParam target) throws ParamsClauseBuilderException{
            if (this.parameter.isRecId()) {
                if (target.isRecId()) { // оба параметра recid - значение должно быть одинаковым
                    String strRecId1 = this.parameter.getValue().toString();
                    String strRecId2 = target.getValue().toString();
                    if (!strRecId1.equals(strRecId2))
                        throw new ParamsClauseBuilderException(String.format(INCOMPATIBLE_RECID, this.parameter.parameterName));
                }
                else { // первый параметр RecId, а второй - нет. убедимся, что второй - пустой
                    if (!target.isNull())
                        throw new ParamsClauseBuilderException(String.format(INCOMPATIBLE_VALUES_RECID_AND_CONST, this.parameter.parameterName));
                }
            }
            else {
                if (target.isRecId()) { // первый параметр - константа, второй - RecId. убедимся, что первый параметр - пустой
                    if (!this.parameter.isNull())
                        throw new ParamsClauseBuilderException(String.format(INCOMPATIBLE_VALUES_RECID_AND_CONST, this.parameter.parameterName));
                }
                else {
                    if (!this.parameter.isNull() && !target.isNull() && !this.parameter.getValue().equals(target.getValue()))
                        throw new ParamsClauseBuilderException(String.format(VARIOUS_VALUES_FOR_PARAMETER, this.parameter.parameterName));
                }
            }
        }

        protected void replaceValueIfNeedFrom(QueryParam target) {

            if (this.parameter.getParamType() != target.getParamType() &&
                    this.parameter.getParamType() != QueryParam.ParamType.UNKNOWN &&
                    target.getParamType() != QueryParam.ParamType.UNKNOWN)
                throw new ParamsClauseBuilderException(String.format(PARAM_TYPES_CONFLICT,
                        this.parameter.getParamType().toString(),
                        target.getParamType().toString(),
                        this.parameter.parameterName));

            if (this.parameter.getParamType() == QueryParam.ParamType.UNKNOWN)
                this.parameter.setParamType(target.getParamType());

            if (this.parameter.isRecId()) {
                // оба параметра содержат одинаковые recid или извлечённый параметр - с пустым или Null-значением
                // ничего не делаем
            }
            else if(target.isRecId())
                // параметр "в кармане" - пустой или Null, извлекли параметр с recid
                // просто копируем значение
                this.parameter.setValueObj((Value) target.getValueObj().getClone());
            else if (this.parameter.isNull()) {
                if (!this.parameter.isNull()) {
                    // параметр "в кармане" пустой или Null
                    // установим ему извлечённое значение
                    this.parameter.setValueType(target.getValueType());
                    this.parameter.setValue(target.getValue());
                }
                else if (this.parameter.getValueType() == FieldTypeId.tid_UNKNOWN)
                    // если тип значения параметра "в кармане" неизвестен - установим тип значения извлечённого параметра
                    this.parameter.setValueType(target.getValueType());
            }
        }
    }
}
