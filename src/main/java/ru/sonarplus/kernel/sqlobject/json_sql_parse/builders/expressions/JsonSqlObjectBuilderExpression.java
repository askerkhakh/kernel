package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.expressions;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.JsonSqlParser;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.*;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.*;

public class JsonSqlObjectBuilderExpression extends JsonSqlObjectBuilder {

    @Override
    public SqlObject parseJsonNode(JSONObject node)
            throws Exception {
        String strExpr = (String) node.get(KEY_CONTENT);
        Preconditions.checkState(!StringUtils.isEmpty(strExpr));
        if (strExpr.equalsIgnoreCase(Expression.NULL))
            // в Delphi NULL-значение сериализуется ак строка "NULL",
            // соответственно здесь такую строку восстанавливаем как NULL-значение
            return ValueConst.createNull();

        JSONArray jsonArgs = getArray(node, KEY_ARGS);
        // выражение представляет собой только безымянную ссылку на единственный аргумент выражения
        if (strExpr.equals(Expression.UNNAMED_ARGUMENT_REF) || strExpr.equals("("  + Expression.UNNAMED_ARGUMENT_REF + ")")) {
            Preconditions.checkNotNull(jsonArgs);
            Preconditions.checkState(jsonArgs.size() == 1);
            JSONObject jsonExprChild = Preconditions.checkNotNull(getObject(jsonArgs, 0, KEY_ARGS));
            SqlObject exprChild = JsonSqlParser.parseJsonNode(jsonExprChild);

            // в Delphi сам объект TScalar в json не сериализуется.
            // для него создаётся и сериализуется выражение-обёртка "(??)",
            // и сериализуется TScalar.Select, как подчинённый выражению-обёртке.
            if (exprChild instanceof Select) {
                // а здесь, обнаружив такую связку "(??)" + Select восстановим как Scalar
                Scalar scalar = new Scalar();
                scalar.setSelect((Select) exprChild);
                return scalar;
            }

            return exprChild;
        }

        Expression expr = new Expression(null);
        expr.setExpr(strExpr);
        // флаг "чистый sql" определяем по наличию в строке выражения специального тега
        expr.isPureSql = !strExpr.contains(ExprConsts.EXPR_BEGIN);

        if (jsonArgs != null)
            for (int i = 0; i < jsonArgs.size(); i++) {
                SqlObject child = JsonSqlParser.parseJsonNode(getObject(jsonArgs, i, KEY_ARGS));
                if (child instanceof Select) {
                    Scalar scalar = new Scalar();
                    scalar.setSelect((Select) child);
                    expr.insertItem(scalar);
                }
                else
                    expr.insertItem(child);
            }
        return expr;
    }

    @Override
    public String getKind() { return KIND_EXPRESSION ;}

}
