package ru.sonarplus.kernel;

import java.text.MessageFormat;
import java.util.Comparator;

/**
 * Описывает параметр в sql-запросе.
 * 
 * @author martynov
 */
public class OracleQueryParameter {

	public static class QueryParameterIndexComparator implements Comparator<OracleQueryParameter>{
		@Override
		public int compare(OracleQueryParameter o1, OracleQueryParameter o2) {
			return o1.getIndex() - o2.getIndex();
		}
	}

	/**
	 * Индекс параметра в запросе
	 */
	private int index;
	
	/**
	 * Имя параметра в запросе
	 */
	private String name;

	/**
	 * &quot;Направление&quot; параметра (in/out)
	 */
	private QueryParameterDirection direction;

	/**
	 * Тип (jdbc.Types или OracleTypes) параметра.
	 */
	private int type;

	/**
	 * Значение параметра (для IN-параметров).
	 */
	private Object value;
	
	private boolean dummy = false;
	
	public OracleQueryParameter(){
		super();
	}
	
	public OracleQueryParameter(final int index, final String name, final int type, final Object value, QueryParameterDirection direction){
		this.index = index;
		this.name = name;
		this.type = type;
		this.value = value;
		this.direction = direction;
	}
	
	
	public OracleQueryParameter(final String name, final int type, final Object value, QueryParameterDirection direction){
		this(0, name, type, value, direction);
	}
	
	public OracleQueryParameter(final String name, final int type){
		this(0, name, type, null, QueryParameterDirection.OUT);
	}
	
	
	public OracleQueryParameter(final OracleQueryParameter template){
		this(template.getIndex(), template.getName(), template.getType(), template.getValue(), template.getDirection());
	}
	
	

	/**
	 * @return the index
	 */
	public int getIndex() {
		return index;
	}

	/**
	 * @param index the index to set
	 */
	public void setIndex(int index) {
		this.index = index;
	}
	
	/**
	 * @return the index
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param index the index to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	public boolean isDummy() {
		return dummy;
	}

	public void setDummy(boolean dummy) {
		this.dummy = dummy;
	}

	/**
	 * @return the direction
	 */
	public QueryParameterDirection getDirection() {
		return direction;
	}

	/**
	 * @param direction the direction to set
	 */
	public void setDirection(QueryParameterDirection direction) {
		this.direction = direction;
	}

	/**
	 * @return the type
	 */
	public int getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(int type) {
		this.type = type;
	}

	/**
	 * @return the value
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(Object value) {
		this.value = value;
	}


	@Override
	public int hashCode() {
		return getIndex();
	}


	@Override
	public boolean equals(Object obj) {
		boolean result = false;
		if(obj instanceof OracleQueryParameter){
			final OracleQueryParameter p = (OracleQueryParameter) obj;
			result = (getIndex() == p.getIndex());
			if(getName()!=null && p.getName()!=null){
				result = result && (getName().equals(p.getName())); 
			} 
		}
		return result; 
	}


	@Override
	public String toString() {
		return MessageFormat.format("index[{0}]name[{1}]type[{2}]direction[{3}]value[{4}]", 
				new Object[]{getIndex(), getName(), getType(), getDirection(), getValue()});
	}
	
	
}
