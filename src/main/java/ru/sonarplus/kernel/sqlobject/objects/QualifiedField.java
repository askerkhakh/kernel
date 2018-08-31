package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;

public class QualifiedField extends ColumnExpression {
	public String alias;
	public String fieldName;


	public QualifiedField(String alias, String name) {
		super();
		this.alias = alias;
		this.fieldName = name;
	}

	public QualifiedField(SqlObject owner, String alias, String name)
			throws SqlObjectException {
		super(owner);
		this.alias = alias;
		this.fieldName = name;
	}
	
	public QualifiedName getQName() {
		return new QualifiedName(alias, fieldName);
	}

}
