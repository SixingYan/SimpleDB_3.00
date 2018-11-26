package simpledb.query;

import java.util.*;

import simpledb.materialize.FunctionFinder;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 * @author Edward Sciore
 */

public class ProjectScan implements Scan {
	private Scan s;
	private Collection<String> fieldlist;

	/**
	* Creates a project scan having the specified
	* underlying scan and field list.
	* @param s the underlying scan
	* @param fieldlist the list of field names
	*/
	public ProjectScan(Scan s, Collection<String> fieldlist) {
		this.s = s;
		this.fieldlist = fieldlist;
	}

	public void beforeFirst() {
		s.beforeFirst();
	}

	public boolean next() {
		return s.next();
	}

	public void close() {
		s.close();
	}
	
	/**
	 * @param fldname the field term of selectList, may contain function like "distance()"
	 * @return the value of field, or calculated by the function
	 */
	public Constant getVal(String fldname) {
		// if this fldname is a self-define function name
		FunctionFinder ff = new FunctionFinder();
		if (ff.isFn(fldname)) {
			for (String fld : ff.getFlds()) {
				if (!hasField(fld))
					throw new RuntimeException("function-field " + fldname + " not found.");
				ff.setFieldVal(s.getVal(fld));
			}
			return ff.excFn();
		}
		// else, it is a normal field
		if (hasField(fldname))
			return s.getVal(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public int getInt(String fldname) {
		if (hasField(fldname))
			return s.getInt(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public String getString(String fldname) {
		if (hasField(fldname))
			return s.getString(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}
	
	public Float getFloat(String fldname) {
		if (hasField(fldname))
			return s.getFloat(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}
	
	/**
	* Returns true if the specified field
	* is in the projection list.
	* @see simpledb.query.Scan#hasField(java.lang.String)
	*/
	public boolean hasField(String fldname) {
		return fieldlist.contains(fldname);
	}
}
