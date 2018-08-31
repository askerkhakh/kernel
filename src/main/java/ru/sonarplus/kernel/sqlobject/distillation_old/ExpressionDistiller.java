package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.db_support.FunctionsReplacer;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExpressionException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ExpressionDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);
	public ExpressionDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof Expression;
	}
	
	protected static class ReplaceFieldsBinToTextContext {
		public boolean isAllQNamesDistilated;
	}
	
	
	protected void collectExprArguments(List<SqlObject> listForDistillate,
			Expression expr) throws ExpressionException {
		for (SqlObject item: expr) {
			if ((!(item instanceof ColumnExpression)) || (item instanceof ValueRecId))
			    throw new ExpressionException(String.format("Недопустимый тип аргумента (\"%s\") в выражении", item.getClass().getSimpleName()));
			listForDistillate.add(item);
		}
	}
	
	protected void distillateExprArguments(Expression expr, DistillerState state)
			throws Exception{
		List<SqlObject> listForDistillate = new ArrayList<SqlObject>();
		collectExprArguments(listForDistillate, expr);
		for (int i = 0; i < listForDistillate.size(); i++) {
			internalDistillateObject(new DistillerState(listForDistillate.get(i),
					new DistillationParamsIn(false), state), COLUMN_EXPRESSION, null);
		}
	}
	
	protected void distillateFieldBinToText(String tableAlias, ColumnExprTechInfo fieldSpec,
			Expression expr,
			DistillerState state)
			throws Exception{
		Expression binToTextExpr = state.dbc.dbSupport.getExpressionForBinaryField(tableAlias, fieldSpec);
		expr.setExpr(binToTextExpr.getExpr());
		expr.insertItem(binToTextExpr.firstSubItem());
		internalDistillateObject(new DistillerState(expr.firstSubItem(), state), null, null);
	}
	
	protected void replaceFieldsBinToText(Expression expr,
			DistillerState state, ReplaceFieldsBinToTextContext context)
			throws Exception{
		String[] binToTextFields = ExprUtils.exprExtractBinToTextFields(expr.getExpr());
		if (binToTextFields.length == 0)
			return;

  	    // после применения к выражению процедуры построения дерева выражений,
	    // выражение FieldBinToText содержится целиком в объекте TExpression,
	    // не содержащем более ничего, в т.ч. подчинённых компонентов-аргументов.
	    // Поэтому количество извлечённых имён полей = 1
		Preconditions.checkState(binToTextFields.length == 1);
		Preconditions.checkState(!expr.isHasChilds());
		
		SqlQuery parentQuery = Utils.getParentQueryEx(expr);
		String sField = binToTextFields[0];
		QualifiedName qName = QualifiedName.stringToQualifiedName(sField);
		ColumnExprTechInfo fieldSpec = state.namesResolver.resolveName(parentQuery, 
				QualifiedName.formQualifiedNameString(
						fieldAliasPart(expr, qName.alias),
						qName.name));
		if (fieldSpec != null)
			distillateFieldBinToText(qName.alias, fieldSpec, expr, state);
		else
			context.isAllQNamesDistilated = false;
	}
	
	protected boolean isSubSelectInFromClause(Select select) {
		SqlObject item = select;
		while ((item.getOwner() != null) &&
		       !(item.getOwner() instanceof CursorSpecification) &&
		       !(item.getOwner() instanceof CommonTableExpression)) {
			if (item.getOwner() instanceof SourceQuery) {
				return true;
			}
			item = item.getOwner();
		}
		return false;
	}
	
	protected void checkPureAsteriskPossibleHere(Expression asterisk) throws ExpressionException {
		if (asterisk.getOwner() instanceof SelectedColumn) {
			SqlObject parentQuery = SqlObjectUtils.getParentQuery(asterisk);
	        // не позволяем настоящую звёздочку в корневом запросе, иначе как будем настраивать поля набора данных?
			Preconditions.checkNotNull(parentQuery.getOwner());
	        // позволяем звёздочку только в подзапросах раздела FROM
			Preconditions.checkArgument(isSubSelectInFromClause((Select)parentQuery));
			
		}
		else {
            if (!(asterisk.getOwner() instanceof Expression))
                throw new ExpressionException(
                        String.format(
                                "Вместо экземпляра класса %s подан %s",
                                Expression.class.getSimpleName(),
                                asterisk.getOwner().getClass().getSimpleName()));
		}
	}
	
    protected void replaceFunctions(Expression expr, DistillerState state)
			throws Exception{
        FunctionsReplacer.execute(state.root, expr, state.dbc.dbSupport);
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		Expression exprOrg = (Expression) state.sqlObject;
		
		if (SqlObjectUtils.isPureAsterisk(exprOrg)) {
			checkPureAsteriskPossibleHere(exprOrg);
			SqlObjectUtils.buildTechInfoForExpr(exprOrg, FieldTypeId.tid_UNKNOWN);
			return exprOrg;
		}
		
 	    //выполнить дистиляцию объектов, подчиненных выражению - его аргументы
		if (!Utils.isTechInfoFilled(exprOrg)) {
			distillateExprArguments(exprOrg, state);
		}
		
		ReplaceFieldsBinToTextContext context = new ReplaceFieldsBinToTextContext();
		context.isAllQNamesDistilated = true;
		
		if (!exprOrg.isPureSql) {
            //избавиться от разметки в строке выражения
            replaceFieldsBinToText(exprOrg, state, context);
            replaceFunctions(exprOrg, state);
		}
		ColumnExpression exprNew = null;
		
		if (exprOrg.getExpr().equals(Expression.UNNAMED_ARGUMENT_REF)) {
			exprNew = (ColumnExpression) exprOrg.firstSubItem();
			replace(state.sqlObject, exprNew);
		}
		else {
            if (ExprUtils.getUnnamedRefCount(exprOrg.getExpr()) != exprOrg.itemsCount())
                throw new ExpressionException(String.format(ExprUtils.NOT_CORRESPONDING_UNNAMED_ARG_REFS_WITH_EXPR_CHILDS, exprOrg.getExpr()));
			exprNew = exprOrg;
		}
		
		if (!context.isAllQNamesDistilated) {
		    // если не удалось разрешить какие-либо квалифицированные имена, используемые в выражении -
		    //    передадим выражение "наверх"
			addNotDistillatedObject(exprNew, state);
		}
		else if (exprNew instanceof Expression) {
		    // ничего не знаю о результате выражения и его регистрозависимости
			SqlObjectUtils.buildTechInfoForExpr(exprNew, FieldTypeId.tid_UNKNOWN);
	        // выставим флаг "чистый sql"
			((Expression)exprNew).isPureSql = true;
		}
		return exprNew;
		
	}

}
