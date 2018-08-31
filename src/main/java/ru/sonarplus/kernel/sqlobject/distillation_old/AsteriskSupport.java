package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.HashMap;
import java.util.Map;

public class AsteriskSupport {
	
	public static void buildAsteriskColumns(QualifiedField asterisk, NamesResolver resolver)
			throws SqlObjectException {
		AsteriskColumnsBuilderDist builder = new AsteriskColumnsBuilderDist(resolver);
		builder.execute(asterisk);
		
	}

	public AsteriskSupport() {

	}
	
	protected static class AsteriskColumnsBuilderCommonContext {
		public String tableName;
	}
	
	protected abstract static class AsteriskColumnsBuilderCommon {
		SelectedColumn column;
		Select parent;
		Map<String, Integer> joins = new HashMap<String, Integer>();
		
		protected abstract ColumnExprTechInfo[] getFields(FromClauseItem fromItem, AsteriskColumnsBuilderCommonContext context);
		
		
		protected void replaceAsteriskWithColumns(SelectedColumnsContainer columns)
				throws SqlObjectException {
			parent.getColumns().replaceWithSet(column, columns.getSubItems());
		}
		
		protected void execute(QualifiedField asterisk)
				throws SqlObjectException {
			Preconditions.checkNotNull(asterisk);
			column = CommonDistiller.getColumnThatContainsThisExpr(asterisk);
			Preconditions.checkNotNull(column, "Знак \"*\" при определении поля может использоваться только в разделе SELECT");
			parent = (Select) SqlObjectUtils.getParentQuery(column);
			Preconditions.checkNotNull(parent);
			SelectedColumnsContainer columnsContainer = internalExecute(asterisk);
			replaceAsteriskWithColumns(columnsContainer);
		}
		
		protected QualifiedField createQFieldWithSpec(String alias, ColumnExprTechInfo spec) {
			QualifiedField result = new QualifiedField(alias, spec.nativeFieldName);
			result.distTechInfo = spec;
			return result;
		}
		
		protected void fillColumns(SelectedColumnsContainer columns, FromClauseItem fromItem,
				String tableAlias)
				throws SqlObjectException {
			AsteriskColumnsBuilderCommonContext context = new AsteriskColumnsBuilderCommonContext();
			ColumnExprTechInfo[] fields = getFields(fromItem, context);
			String tableIdent = StringUtils.isEmpty(tableAlias) ? fromItem.getAliasOrName() : tableAlias;
			if (fields != null) {
				for (ColumnExprTechInfo item: fields) {
					columns.addColumn(createQFieldWithSpec(tableIdent, item), "");
				}
			}
			
		}
		
		protected int getCountOfJoins(FromClauseItem fromItem) {
			if (!(fromItem.getTableExpr().getSource() instanceof SourceTable)) {
				return 0;
			}
			String tableName = ((SourceTable)fromItem.getTableExpr().getSource()).table;
			Integer result = joins.get(tableName);
			if (result == null) {
				result = 0;
			}
			joins.put(tableName, result+1);
			return result;
		}
		
		protected SelectedColumnsContainer internalExecute(QualifiedField asterisk)
				throws SqlObjectException {
			SelectedColumnsContainer result = new SelectedColumnsContainer();
			String tableAlias = asterisk.alias.trim();
			FromClauseItem fromItem;
			if (StringUtils.isEmpty(tableAlias)) {
				for (SqlObject item: parent.getFrom()) {
					fromItem = (FromClauseItem) item;
					fillColumns(result, fromItem, "");
				}
			}
			else {
				fromItem = parent.getFrom().findItem(tableAlias);
				fillColumns(result, fromItem, tableAlias);
			}
			return result;
		}
	}
	
	protected static class AsteriskColumnsBuilderDist extends AsteriskColumnsBuilderCommon {
		NamesResolver resolver;
		
		public AsteriskColumnsBuilderDist(NamesResolver resolver) {
			Preconditions.checkNotNull(resolver);
			this.resolver = resolver;
		}

		@Override
		protected ColumnExprTechInfo[] getFields(FromClauseItem fromItem, AsteriskColumnsBuilderCommonContext context) {
			Preconditions.checkArgument((fromItem != null) && (fromItem.getTableExpr() != null));
			Source source = fromItem.getTableExpr().getSource();
			if (source instanceof SourceTable) {
				context.tableName = ((SourceTable) source).table;
			}
			else if (source instanceof SourceQuery) {
				context.tableName = fromItem.getAlias();
			}
			else {
				Preconditions.checkArgument(false);
			}
			return resolver.asteriskFields(parent, fromItem.getAliasOrName());
		}
		
	}

}
