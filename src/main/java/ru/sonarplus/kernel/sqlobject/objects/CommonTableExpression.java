package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CommonTableExpression extends SqlObject {
	protected Select select = null;
    protected CTECycleClause cycle = null;

	public String alias;
	public List<String> columns = new ArrayList<String>();

    public CommonTableExpression() { super(); }
    public CommonTableExpression(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

	public CTECycleClause getCycle() {
		return this.cycle;
	}

	public CommonTableExpression setCycle(CTECycleClause value)
            throws SqlObjectException {
        if (this.cycle != value) {
            unassignItem(cycle);
            cycle = assignItem(value);
        }
        return this;
	}

	public Select findSelect() {
		return this.select;
	}
	
	public Select getSelect() { return Preconditions.checkNotNull(select); }

	public CommonTableExpression setSelect(Select value)
            throws SqlObjectException {
        if (this.select != value) {
            unassignItem(this.select);
            this.select = assignItem(value);
        }
		return this;
	}

	public CommonTableExpression setColumns(String... value) {
		columns =  Arrays.asList(value);
		return this;
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        CommonTableExpression targetCTE = (CommonTableExpression)target;

        targetCTE.select = select == null ? null : target.setOwner((Select)select.clone());
        targetCTE.cycle = cycle == null ? null : target.setOwner((CTECycleClause)cycle.clone());
    }

    @Override
    protected void internalInsertItem(SqlObject item)
            throws SqlObjectException {
        if (item.getClass() == Select.class) {
            if (this.select == null)
                this.select = (Select) item;
            else
                super.internalInsertItem(item);
        }
        else if (item.getClass() == CTECycleClause.class) {
            if (this.cycle == null)
                this.cycle = (CTECycleClause) item;
            else
                super.internalInsertItem(item);
        }
        else
            super.internalInsertItem(item);

    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.select) {
            unassignItem(this.select); this.select = null;
        }
        else if (item == this.cycle) {
            unassignItem(this.cycle); this.cycle = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.select)
            this.select = (Select) with;
        else if (what == this.cycle)
            this.cycle = (CTECycleClause) with;
        else
          super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(select); select = null;
        unassignItem(cycle); cycle = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{select, cycle};
    }

}
