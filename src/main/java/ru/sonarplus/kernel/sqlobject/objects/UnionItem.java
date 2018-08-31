package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.Map;
import java.util.TreeMap;

public class UnionItem extends SqlObject {

    public enum UnionType {
        UNION("union"),
        UNION_ALL("union all"),
        INTERSECT("intersect"),
        MINUS("minus"); // вообще говоря Oracle'вый MINUS соответствует EXCEPT в Ansi Sql

        private String text;
        UnionType(String text) { this.text = text; }
        static final Map<String, UnionType> UNIONS_MAP = new TreeMap(String.CASE_INSENSITIVE_ORDER) {
            {
                for (UnionType item: UnionType.values()) {
                    put(item.text, item);
                }
                put("except", MINUS);
            }
        };

        public static UnionType fromString(String text) {
            return Preconditions.checkNotNull(UNIONS_MAP.get(text));
        }

        public String toString() {
            return text;
        }

    }

    protected Select select = null;

	public UnionType unionType = UnionType.UNION_ALL;

    public UnionItem() { super(); }

    public UnionItem(Select select, UnionType unionType)
            throws SqlObjectException {
        super();
        this.unionType = unionType;
        setSelect(select);
    }

    public UnionItem(Select select)
            throws SqlObjectException {
        this(select, UnionType.UNION_ALL);
    }

    public UnionItem(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

	public UnionItem(SqlObject owner, Select select,
			UnionType unionType)
            throws SqlObjectException {
		this(owner);
		this.unionType = unionType;
		setSelect(select);
	}

	public UnionItem(SqlObject owner, Select select)
            throws SqlObjectException {
		this(owner, select, UnionType.UNION_ALL);
	}
	
	public Select findSelect() { return select; }

	public Select getSelect() {return Preconditions.checkNotNull(select); }

	public void setSelect(Select value)
            throws SqlObjectException {
		if (select != value) {
		    unassignItem(select);
		    select = assignItem(value);
        }
	}

    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        UnionItem targetInstance = (UnionItem) target;
        targetInstance.select = select == null ? null : target.setOwner((Select) select.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item.getClass() == Select.class) {
            if (this.select == null)
                this.select = (Select) item;
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
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace (SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.select)
            this.select = (Select) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(select); select = null;
    }

    @Override
    public SqlObject[] getSubItems () {
        return new SqlObject[]{select};
    }
}
