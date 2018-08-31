package ru.sonarplus.kernel.sqlobject.objects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.expressions.ExprConsts;

public class Expression extends ColumnExpression {

    private ExpressionsArray items = null;

    protected class ExpressionsArray extends SqlObjectsArrayInternal {

        public ExpressionsArray(SqlObject owner) { super(owner); }

        @Override
        protected Class getItemClass () { return ColumnExpression.class; }

    }

    public static final char CHAR_BEFORE_PARAMETER = ':';
    public static final char CHAR_BEFORE_TOKEN = '?';
    public static final String UNNAMED_ARGUMENT_REF = "??";
    public static final String ASTERISK = "*";
    public static final String NULL = "NULL";
    public static final String ONE = "1";

    protected String expr;
    public boolean isPureSql;

    public Expression() { super(); }

    public Expression(String expr, boolean isPureSql) {
        this();
        this.expr = expr;
        this.isPureSql = isPureSql;
    }

    public Expression(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}

    public Expression(SqlObject owner, String expr, boolean isPureSql)
            throws SqlObjectException {
		this(owner);
		this.expr = expr;
		this.isPureSql = isPureSql;
	}
    
    public String getExpr() { return expr; }
    
    public void setExpr(String expr){
    	this.expr = expr;
    	isPureSql = expr.indexOf(ExprConsts.EXPR_BEGIN) == -1;
    }
    
    public String getPureAsteriskAlias() {
        if (expr.equals(ASTERISK))
            return "";
        if (expr.endsWith("." + ASTERISK))
            return expr.substring(0, expr.length() - 2);
        else
            return null;
    }

    public boolean isPureAsterisk() {
        return expr.endsWith(ASTERISK);
    }

    public ColumnExpression findById(String id) {
        if (StringUtils.isEmpty(id) || items == null)
            return null;
        for (SqlObject child: items)
            if (id.equals(((ColumnExpression)child).id))
                return (ColumnExpression)child;
        return null;
    }

    protected ExpressionsArray newItems () {
        if (items == null)
            items = new ExpressionsArray(this);
        return items;
    }

    public final boolean isHasChilds() { return itemsCount() > 0; }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        String id = what == null ? null : ((ColumnExpression)what).id;
        Preconditions.checkNotNull(items).internalReplace(what, with);
        if (with != null)
            ((ColumnExpression)with).id = id;
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {
        newItems().internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        Preconditions.checkNotNull(items).internalRemoveItem(item);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        if (items != null)
            items.destroyItems();
    }

    public int indexOf(SqlObject item) {
        if (items == null)
            return -1;
        else
            return items.indexOf(item);
    }

    public int itemsCount() {
        if (items == null)
            return 0;
        else
            return items.itemsCount();
    }

    public final SqlObject getItem(int index)
            throws SqlObjectException {
        if (items == null)
            throw new SqlObjectException("");
        return items.getItem(index);
    }

    public SqlObject firstSubItem() {
        if (items == null)
            return null;
        else
            return items.firstSubItem();
    }

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        Expression targetInstance = (Expression)target;
        targetInstance.items = null;
        if (this.items != null && this.itemsCount() != 0)
            for (SqlObject item: this.items) {
                SqlObject itemClone = item.clone();
                try {
                    targetInstance.newItems().internalInsertItem(itemClone);
                    targetInstance.setOwner(itemClone);
                }
                catch (SqlObjectException e) {
                    Preconditions.checkState(false,
                            e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
    }

    public final void insertItem(SqlObject item, int index)
            throws SqlObjectException {
        if ((item == null) || (item.owner == this) || (item == this))
            return;
        if (indexOf(item) >= 0)
            return;

        if (!(item instanceof Predicate))
            throw new SqlObjectException(String.format(
                    "Объект '%s' не может содержать подчинённые '%s'",
                    getClass().getSimpleName(),
                    item.getClass().getSimpleName()
            ));

        checkCycle(item);
        if (item.owner != null)
            item.owner.removeItem(item);

        newItems().insertItem(item, index);
    }

    @Override
    public SqlObject[] getSubItems () {
        if (items != null)
            return items.getSubItems();
        else
            return new SqlObject[]{};
    }

}
