package simpledb.parse;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.FunctionFinder;
import simpledb.query.*;
import java.util.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore | Sixing Yan
 */
public class QueryData {
	private Collection<String> fields;
	private Collection<String> tables;
	private Predicate pred;
	private Collection<String> groupfields;
	private Collection<AggregationFn> aggfns;
	/**
	* Saves the field and table list and predicate.
	*/
	public QueryData(Collection<String> fields, Collection<String> tables, Predicate pred) {
		this.fields = fields;
		this.tables = tables;
		this.pred = pred;
	}

	/**
	* Returns the fields mentioned in the select clause.
	* @return a collection of field names
	*/
	public Collection<String> fields() {
		return fields;
	}

	/**
	* Returns the tables mentioned in the from clause.
	* @return a collection of table names
	*/
	public Collection<String> tables() {
		return tables;
	}

	/**
	* Returns the predicate that describes which
	* records should be in the output table.
	* @return the query predicate
	*/
	public Predicate pred() {
		return pred;
	}
	
	/**
	 * 
	 * @return
	 */
	public Collection<String> groupfields() {
		return this.groupfields;
	}
	
	/**
	 * 
	 * @return
	 */
	public Collection<AggregationFn> aggfns () {
		return this.aggfns;
	}

	public void setGroupByFields (Collection<String> groupfld) {
		this.groupfields = groupfld;
	}

	public void setAggfns (Collection<String> aggfns) {
		FunctionFinder ff = new FunctionFinder();
		for (String fn : aggfns)
			if (ff.hasAggFn(fn))
				this.aggfns.add(ff.getAggFn());
	}

	public boolean hasGroupBy() {
		return this.groupfields != null;
	}

	public String toString() {
		// no available now
		String result = "select ";
		for (String fldname : fields)
			result += fldname + ", ";
		result = result.substring(0, result.length() - 2); //remove final comma
		result += " from ";
		for (String tblname : tables)
			result += tblname + ", ";
		result = result.substring(0, result.length() - 2); //remove final comma
		String predstring = pred.toString();
		if (!predstring.equals(""))
			result += " where " + predstring;
		return result;
	}
}
