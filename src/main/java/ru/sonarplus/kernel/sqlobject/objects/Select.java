package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import javax.annotation.Nullable;

public class Select extends SqlQuery {

    @Nullable
    private SelectedColumnsContainer columns;
    @Nullable
    private FromContainer from;
    @Nullable
    private Conditions where;
    @Nullable
    private GroupBy groupBy;
    @Nullable
    private UnionsContainer unions;
    @Nullable
    private CTEsContainer clauseWith;


	public boolean distinct = false;

    public Select() { super(); }
    public Select(SqlObject owner)
            throws SqlObjectException { super(owner); }

	public TableExpression getBaseTableExpr(){
        FromClauseItem item = null;
        if (from != null && from.isHasChilds())
            item = (FromClauseItem)from.firstSubItem();

		Preconditions.checkNotNull(item,
				"В запросе %s отсутствует основное табличное выражение",
				this.getClass().getSimpleName());
		
		return item.getTableExpr();	
	}
	
	public Source getBaseSource()
            throws SqlObjectException {
		TableExpression expr = getBaseTableExpr();
		Source result = expr.getSource();
		Preconditions.checkNotNull(result,
				"В запросе %s не задан источник записей"+
		        " основного табличного выражения \"%s\"",
		        this.getClass().getSimpleName(),
		        expr.alias);
		return result;
	}
	
    @Nullable
	public SelectedColumnsContainer findColumns() { return columns; }

    public SelectedColumnsContainer getColumns() {
        return Preconditions.checkNotNull(columns);
    }

	public SelectedColumnsContainer newColumns() {
        if (columns == null)
            columns = setOwner(new SelectedColumnsContainer());
		return columns;
	}

	public Select setColumns(SelectedColumnsContainer container)
            throws SqlObjectException {
        unassignItem(columns);
        columns = assignItem(container);
        return this;
	}

    @Nullable
	public FromContainer findFrom() {
		return from;
	}

    public FromContainer getFrom() {
        return Preconditions.checkNotNull(from);
    }

	public FromContainer newFrom()  {
        if (from == null)
            from = setOwner(new FromContainer());
        return from;
	}

    public Select setFrom(FromContainer value)
            throws SqlObjectException {
        unassignItem(from);
        from = assignItem(value);
        return this;
    }

    @Nullable
    public Conditions findWhere() { return where; }

    public Conditions getWhere() {
        return Preconditions.checkNotNull(where);
    }

    public Conditions newWhere() {
        if (where == null)
            where = setOwner(new Conditions());
        return where;
    }

    public Select setWhere(Conditions value)
            throws SqlObjectException {
        unassignItem(where);
        where = assignItem(value);
        return this;
    }

    @Nullable
	public GroupBy findGroupBy() { return groupBy; }

    public GroupBy getGroupBy() {
        return Preconditions.checkNotNull(groupBy);
    }

	public GroupBy newGroupBy() {
        if (groupBy == null)
            groupBy = setOwner(new GroupBy());
        return groupBy;
	}

	public void setGroupBy(GroupBy value)
            throws SqlObjectException {
        unassignItem(groupBy);
        groupBy = assignItem(value);
	}

    @Nullable
	public UnionsContainer findUnions() { return unions; }

    public UnionsContainer getUnions() {
        return Preconditions.checkNotNull(unions);
    }

	public UnionsContainer newUnions() {
        if (unions == null)
            unions = setOwner(new UnionsContainer());
        return unions;
	}

	public void setUnions(UnionsContainer value)
            throws SqlObjectException {
        unassignItem(unions);
        unions = assignItem(value);
    }

    @Nullable
	public CTEsContainer findWith() {
		return clauseWith;
	}

    public CTEsContainer getWith() {
        return Preconditions.checkNotNull(clauseWith);
    }

	public CTEsContainer newWith() {
        if (clauseWith == null)
            clauseWith = setOwner(new CTEsContainer());
        return clauseWith;
	}

	public void setWith(CTEsContainer value)
            throws SqlObjectException {
        unassignItem(clauseWith);
        clauseWith = assignItem(value);
	}

	public CursorSpecification toCursorSpecification(boolean clone)
            throws SqlObjectException, CloneNotSupportedException {
		CursorSpecification result = new CursorSpecification(null); 
		if (clone) {
			SqlObject newClone = clone();
			result.setSelect((Select)newClone);
		}
		else
			result.setSelect(this);

		QueryParams params = result.getSelect().getParams();
		if (params != null)
			result.setParams(params);

		return result;
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        Select targetInstance = (Select) target;
        targetInstance.clauseWith = clauseWith == null ? null : target.setOwner((CTEsContainer) clauseWith.clone());
        targetInstance.columns = columns == null ? null : target.setOwner((SelectedColumnsContainer) columns.clone());
        targetInstance.from = from == null ? null : target.setOwner((FromContainer) from.clone());
        targetInstance.where = where == null ? null : target.setOwner((Conditions) where.clone());
        targetInstance.groupBy = groupBy == null ? null : target.setOwner((GroupBy) groupBy.clone());
        targetInstance.unions = unions == null ? null : target.setOwner((UnionsContainer) unions.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == SelectedColumnsContainer.class) {
            if (this.columns == null)
                this.columns = (SelectedColumnsContainer) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == FromContainer.class) {
            if (this.from == null)
                this.from = (FromContainer) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == Conditions.class) {
            if (this.where == null)
                this.where = (Conditions) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == GroupBy.class) {
            if (this.groupBy == null)
                this.groupBy = (GroupBy) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == CTEsContainer.class) {
            if (this.clauseWith == null)
                this.clauseWith = (CTEsContainer) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == UnionsContainer.class) {
            if (this.unions == null)
                this.unions = (UnionsContainer) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == clauseWith)
            clauseWith = (CTEsContainer) with;
        else if (what == columns)
            columns = (SelectedColumnsContainer) with;
        else if (what == from)
            from = (FromContainer) with;
        else if (what == where)
            where = (Conditions) with;
        else if (what == groupBy)
            groupBy = (GroupBy) with;
        else if (what == unions)
            unions = (UnionsContainer) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(clauseWith); clauseWith = null;
        unassignItem(columns); columns = null;
        unassignItem(from); from = null;
        unassignItem(where); where = null;
        unassignItem(groupBy); groupBy = null;
        unassignItem(unions); unions = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{params, clauseWith, columns, from, where, groupBy, unions};
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.where) {
            unassignItem(this.where); this.where = null;
        }
        else if (item == this.clauseWith) {
            unassignItem(this.clauseWith); this.clauseWith = null;
        }
        else if (item == this.columns) {
            unassignItem(this.columns); this.columns = null;
        }
        else if (item == this.from) {
            unassignItem(this.from); this.from = null;
        }
        else if (item == this.groupBy) {
            unassignItem(this.groupBy); this.groupBy = null;
        }
        else if (item == this.unions) {
            unassignItem(this.unions); this.unions = null;
        }
        else
            super.internalRemoveItem(item);
    }

}
