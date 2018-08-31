package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;

import java.util.Map;
import java.util.TreeMap;

public class Join extends SqlObject {

    public enum JoinType {
        LEFT ("left join"),
        RIGHT ("right join"),
        INNER ("inner join"),
        FULL_OUTER ("full outer join");

        private String text;
        JoinType(String text) { this.text = text; }
        static final Map<String, JoinType> ITEMS_MAP = new TreeMap(String.CASE_INSENSITIVE_ORDER) {
            {
                for (JoinType item: JoinType.values())
                    put(item.text, item);
                put("full join", FULL_OUTER);
            }
        };

        public static JoinType fromString(String text) {

            return Preconditions.checkNotNull(ITEMS_MAP.get(text));
        }

        public String toString() {
            return text;
        }

    }

	protected Predicate joinOn = null;
	public JoinType joinType = JoinType.LEFT;

    public Join() { super(); }

    public Join(Predicate joinOn, JoinType joinType)
            throws SqlObjectException {
        this();
        setJoinOn(joinOn);
        this.joinType = joinType;
    }

    public Join(Predicate joinOn)
            throws SqlObjectException { this(joinOn, JoinType.LEFT); }

	public Join(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}

	public Join(SqlObject owner, Predicate joinOn, JoinType joinType)
            throws SqlObjectException {
		this(owner);
		this.joinType = joinType;
		setJoinOn(joinOn);
	}

	public Join(SqlObject owner, Predicate joinOn)
            throws SqlObjectException {
		this(owner, joinOn, JoinType.LEFT);
	}
	
	public Join(
			QualifiedName[] fieldsLeft,
			QualifiedName[] fieldsRight,
			JoinType joinType,
			boolean isRFields)
            throws SqlObjectException {
		super();
		Preconditions.checkNotNull(fieldsLeft);
		Preconditions.checkNotNull(fieldsRight);
		Preconditions.checkState(fieldsLeft.length == fieldsRight.length);
		this.joinType = joinType;
		Conditions cond = new Conditions(Conditions.BooleanOp.AND);
		setJoinOn(cond);
		for (int i = 0; i < fieldsLeft.length; i++) {
			QualifiedName left = fieldsLeft[i];
			QualifiedName right = fieldsRight[i];
			
			QualifiedField leftArg = null;
			QualifiedField rightArg = null;
			if (isRFields) {
				leftArg = new QualifiedRField(null, left.alias, left.name);
				rightArg = new QualifiedRField(null, right.alias, right.name);
			}
			else {
				leftArg = new QualifiedField(null, left.alias, left.name);
				rightArg = new QualifiedField(null, right.alias, right.name);
				
			}
			new PredicateComparison(cond, leftArg, rightArg, 
					PredicateComparison.ComparisonOperation.EQUAL);
		}
		
	}
	
	public Predicate getJoinOn() {
		return joinOn;
	}

	public void setJoinOn(Predicate value)
            throws SqlObjectException {
		if (joinOn != value) {
		    unassignItem(joinOn);
		    joinOn = assignItem(value);
        }
	}


    @Override
    protected void internalClone(SqlObject target)
            throws CloneNotSupportedException {
        super.internalClone(target);
        Join targetInstance = (Join) target;
        targetInstance.joinOn = joinOn == null ? null : target.setOwner((Predicate) joinOn.clone());
    }

    @Override
    protected void internalInsertItem (SqlObject item)
            throws SqlObjectException {

        if (item instanceof Predicate) {
            if (this.joinOn == null)
                this.joinOn = (Predicate) item;
            else
                super.internalInsertItem(item);
        }
        else
          super.internalInsertItem(item);
    }

    @Override
    protected void internalRemoveItem (SqlObject item)
            throws SqlObjectException {
        if (item == this.joinOn) {
            unassignItem(this.joinOn); this.joinOn = null;
        }
        else
            super.internalRemoveItem(item);
    }

    @Override
    protected void internalReplace(SqlObject what, SqlObject with)
            throws SqlObjectException {
        if (what == this.joinOn)
            this.joinOn = (Predicate) with;
        else
            super.internalReplace(what, with);
    }

    @Override
    protected void internalDestroyItems() {
        super.internalDestroyItems();
        unassignItem(joinOn); joinOn = null;
    }

	@Override
	public SqlObject[] getSubItems () {
		return new SqlObject[]{joinOn};
	}

}
