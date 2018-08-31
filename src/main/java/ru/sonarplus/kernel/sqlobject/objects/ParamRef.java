package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class ParamRef extends Parameter {

	public ParamRef() { super(); }

	public ParamRef(String name) {
		this();
		this.parameterName = name;
	}

	public ParamRef(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}

	public ParamRef(SqlObject owner, String name)
            throws SqlObjectException {
		this(owner);
		this.parameterName = name;
	}


}
