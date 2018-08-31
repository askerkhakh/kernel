package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.Map;
import java.util.TreeMap;

public class PredicateComparison extends PredicateComparisonCommon {
	public ComparisonOperation comparison = ComparisonOperation.EQUAL;

    public enum ComparisonOperation {
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS("<"),
        GREAT(">"),
        LESS_EQUAL("<="),
        GREAT_EQUAL(">=");

        private String text;
        ComparisonOperation(String text) { this.text = text; }
        static final Map<String, ComparisonOperation> OPERATIONS_MAP = new TreeMap(String.CASE_INSENSITIVE_ORDER) {
            {
                for (ComparisonOperation item: ComparisonOperation.values()) {
                    put(item.text, item);
                }
            }
        };

        public static ComparisonOperation fromString(String text) {

            return Preconditions.checkNotNull(OPERATIONS_MAP.get(text));
        }

        public String toString() {
            return text;
        }
    }

	public PredicateComparison() {
	    super();
	}

    public PredicateComparison(ComparisonOperation comparison) {
        super();
        this.comparison = comparison;
    }

    public PredicateComparison(ColumnExpression left, ColumnExpression right, ComparisonOperation comparison)
            throws SqlObjectException {
        this(comparison);
        setLeft(left);
        setRight(right);
    }

	public PredicateComparison(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

    public PredicateComparison(SqlObject owner,
                               ComparisonOperation comparison
    )
            throws SqlObjectException {
        this(owner);
        this.comparison = comparison;
    }

    public PredicateComparison(SqlObject owner,
            ColumnExpression left, ColumnExpression right,
			ComparisonOperation comparison
    )
            throws SqlObjectException {
		this(owner, comparison);
        setLeft(left);
        setRight(right);
	}
}
