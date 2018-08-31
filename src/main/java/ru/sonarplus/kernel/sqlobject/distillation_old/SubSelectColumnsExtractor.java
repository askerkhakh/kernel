package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;

public class SubSelectColumnsExtractor {
	protected DbSchemaSpec schema;
	protected Select select;
	

	public SubSelectColumnsExtractor(DbSchemaSpec schema, Select select) {
		this.schema = schema;
		this.select = select;
	}
	
	public ColumnExprTechInfo[] execute()
			throws CloneNotSupportedException, SqlObjectException {
		ColumnExprTechInfo[] result = internalExecute(select);
		for (int i = 0; i < result.length; i++) {
			result[i].resetIndexInfo();
		}
		return result;
	}
	
	protected ColumnExprTechInfo[] extractColumns(Select select) throws CloneNotSupportedException {
		SelectedColumnsContainer selectColumns = select.getColumns();
		ColumnExprTechInfo[] result = new ColumnExprTechInfo[selectColumns.itemsCount()];
		int i = 0;
		for (SqlObject item: selectColumns) {
			SelectedColumn column = (SelectedColumn) item; 
			result[i] = SqlObjectUtils.getTechInfo(column.getColExpr()).clone();
			if (!StringUtils.isEmpty(column.alias)) {
				result[i].dbdFieldName = column.alias;
				result[i].nativeFieldName = column.alias;
			}
			i++;
		}
		return result;
	}
	
	protected ColumnState getColumnsState(Select select) {
		int asterisk = 0;
		int qAsterisk = 0;
		int someColumn = 0;
		for (SqlObject item: select.getColumns()) {
			SelectedColumn column = (SelectedColumn) item;
			String alias = Utils.getPureAsteriskAlias(column.getColExpr());
			if (alias != null) {
				if (StringUtils.isEmpty(alias)) {
					asterisk++;
				}
				else {
					qAsterisk++;
				}
			}
			else {
				someColumn++;
			}
		}
		switch (asterisk) {
		case 0:
			if (qAsterisk == 0) {
  	            // перечень обычных полей
				return ColumnState.COLUMNS;
			}
			else if (someColumn == 0) {
				// квалифицированные звёздочки (одна или более)
				return ColumnState.QASTERISKS;
			}
			else {
				return ColumnState.COLUMNS_AND_QASTERISKS;
			}
		case 1:
			Preconditions.checkState((qAsterisk == 0) && (someColumn == 0));
			return ColumnState.ASTERISK;
		default:
			Preconditions.checkState(false);
			return null;
		}
		
	}
	
	
	
	protected ColumnExprTechInfo[] extractAsteriskFromTable(FromClauseItem fromItem)
			throws CloneNotSupportedException, SqlObjectException {
		return Utils.extractColumnsFromTableOrCTE(schema, fromItem);
	}
	
	protected ColumnExprTechInfo[] extractAsteriskFrom(FromClauseItem fromItem)
			throws CloneNotSupportedException, SqlObjectException {
		Source source = fromItem.getTableExpr().getSource();
		Preconditions.checkNotNull(source);
		if (source instanceof SourceTable) {
			return extractAsteriskFromTable(fromItem);
		}
		else if (source instanceof SourceQuery) {
			return internalExecute( ((SourceQuery) source).findSelect());
		}
		else {
			Preconditions.checkArgument(false, "extractAsteriskFrom не реализован для класса" + 
					source.getClass().getSimpleName() );
			return null;
		}
	}
	
	protected ColumnExprTechInfo[] extractAsterisk(Select select)
			throws CloneNotSupportedException, SqlObjectException {
		List<ColumnExprTechInfo[]> resultItems = new ArrayList<ColumnExprTechInfo[]>();
		for (SqlObject item: select.getFrom()) {
			FromClauseItem fromItem = (FromClauseItem) item;
			ColumnExprTechInfo[] resultItem = extractAsteriskFrom(fromItem);
			if (resultItem != null) {
				resultItems.add(resultItem);
			}
		}
		return Utils.combineTechInfos(resultItems);
	}
	
	protected ColumnExprTechInfo[] extractColumnsAndQAsterisks(Select select)
			throws CloneNotSupportedException, SqlObjectException {
		List<ColumnExprTechInfo[]> resultItemsList = new ArrayList<ColumnExprTechInfo[]>();
		FromContainer fromClause = select.getFrom();
		ColumnExprTechInfo[] resultItem;
		for (SqlObject item: select.getColumns()) {
			SelectedColumn column = (SelectedColumn) item; 
			String alias = Utils.getPureAsteriskAlias(column.getColExpr());
			if (alias != null) {
				resultItem = extractAsteriskFrom(fromClause.findItem(alias));
				if (resultItem != null) {
					resultItemsList.add(resultItem);
				}
			}
			else {
				ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(column.getColExpr()).clone();
				if (!StringUtils.isEmpty(column.alias)) {
					techInfo.dbdFieldName = column.alias;
					techInfo.nativeFieldName = column.alias; 
				}
				resultItemsList.add(new ColumnExprTechInfo[]{techInfo});
			}
		}
		return Utils.combineTechInfos(resultItemsList);
	}
	
	protected ColumnExprTechInfo[] internalExecute(Select select)
			throws CloneNotSupportedException, SqlObjectException {
		switch (getColumnsState(select)) {
		case COLUMNS:
			return extractColumns(select);
		case ASTERISK:
			return extractAsterisk(select);
		case QASTERISKS:
			return extractColumnsAndQAsterisks(select);
		case COLUMNS_AND_QASTERISKS:
			return extractColumnsAndQAsterisks(select);
		default:
			Preconditions.checkArgument(false);
			return null;
			
		}
	}
}
