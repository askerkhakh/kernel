package ru.sonarplus.kernel.dbschema;

public class ForeignKeyConstraintSpec extends ConstraintWithDetailsSpec {
	public boolean useCascadeDelete;
	public boolean useCascadeUpdate;
	public boolean hasValueEdit;
	public ConstraintSpec targetConstraint;
	public String foreignConstraintKind;

	public ForeignKeyConstraintSpec() {
		// TODO Auto-generated constructor stub
	}
	
	public boolean getUseCascadeDelete() {
		return useCascadeDelete;
	}
	
	public boolean getUseCascadeUpdate() {
		return useCascadeUpdate;
	}
	
	public boolean getHasValueEdit() {
		return hasValueEdit;
	}
	
	public ConstraintSpec getTargetConstraint() {
		return targetConstraint;
	}

}
