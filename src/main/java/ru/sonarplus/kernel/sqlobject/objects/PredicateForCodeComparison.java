package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.Map;
import java.util.TreeMap;

public class PredicateForCodeComparison extends PredicateComparisonCommon {

	public enum ComparisonCodeOperation {
		CODE_VASSAL ("code_vassal"),
		ROOT_VASSAL ("root_vassal"),
		CODE_LEAF ("code_leaf"),
		ROOT_LEAF ("root_leaf"),
		CODE_ALL ("code_all"),
		ROOT_ALL ("root_all");
		private String text;

		ComparisonCodeOperation(String text) { this.text = text; }
		static final Map<String, ComparisonCodeOperation> OPERATIONS_MAP = new TreeMap(String.CASE_INSENSITIVE_ORDER) {
			{
				for (ComparisonCodeOperation item: ComparisonCodeOperation.values())
					put(item.text, item);
			}
		};

		public static ComparisonCodeOperation fromString(String text) {
			return Preconditions.checkNotNull(OPERATIONS_MAP.get(text));
		}

		public String toString() {
			return text;
		}

	}

	public ComparisonCodeOperation comparison = ComparisonCodeOperation.ROOT_ALL;


	public PredicateForCodeComparison() { super(); }

	public PredicateForCodeComparison(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

	public PredicateForCodeComparison(QualifiedField field,
									  ColumnExpression code,
									  ComparisonCodeOperation comparison
	)
            throws SqlObjectException {
		super();
		setField(field);
		setCode(code);
		this.comparison = comparison;
	}

	public PredicateForCodeComparison(SqlObject owner,
			QualifiedField field, ColumnExpression code,
			ComparisonCodeOperation comparison
			)
            throws SqlObjectException {
		this(owner);
		setField(field);
		setCode(code);
		this.comparison = comparison;
	}

	public QualifiedField getField() {
		return (QualifiedField) getLeft();
	}
	
	public void setField(QualifiedField field)
            throws SqlObjectException {
	    setLeft(field);
	}
	
	public ColumnExpression getCode() {	return getRight(); }
	
	public void setCode(ColumnExpression value)
            throws SqlObjectException {
		setRight(value);
	}
}
