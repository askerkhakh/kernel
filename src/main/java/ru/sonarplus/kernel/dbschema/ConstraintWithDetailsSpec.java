package ru.sonarplus.kernel.dbschema;

import java.util.ArrayList;
import java.util.List;


public class ConstraintWithDetailsSpec extends ConstraintSpec {
	public List<FieldSpec> items = new ArrayList<FieldSpec>();

	public ConstraintWithDetailsSpec() {
		// TODO Auto-generated constructor stub
	}
	
	public int getItemsCount() {
		return items.size();
	}
	
	public FieldSpec getItem(int index) {
		return items.get(index);
	}

}
