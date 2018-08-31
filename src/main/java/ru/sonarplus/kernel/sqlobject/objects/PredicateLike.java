package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateLike extends PredicateTextSearch {
	public String escape;

	public PredicateLike() { super(); }

	public PredicateLike(ColumnExpression expr,
                         ColumnExpression template,
                         String escape
    )
            throws SqlObjectException {
		this();
		setExpr(expr);
        setTemplate(expr);
        this.escape = escape;
	}

    public PredicateLike(String escape) {
        this();
        this.escape = escape;
    }

	public PredicateLike(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

	public PredicateLike(SqlObject owner,
			ColumnExpression expr,
			ColumnExpression template,
			String escape)
            throws SqlObjectException {
		this(owner);
        setExpr(expr);
        setTemplate(template);
        this.escape = escape;
	}
	
	public PredicateLike(SqlObject owner, String escape)
            throws SqlObjectException {
		this(owner);
		this.escape = escape;
	}

}
