package simpledb.materialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import simpledb.query.Constant;
import simpledb.query.FloatConstant;

/**
 * 
 * @author Sixing Yan
 *
 */
public class FunctionFinder {
	private AggregationFn aggfn = null;
	private Map<String, ArrayList<String>> fnFlds;
	private String fn;
	private ArrayList<Constant> fldvals;
	public static float SELF_LOG = 0;
	public static float SELF_LAT = 0;

	/**
	 * 
	 */
	public FunctionFinder() {
		initFnFlds();
	}
	/**
	 * 
	 * @return
	 */
	public AggregationFn getAggFn() {
		return this.aggfn;
	}

	public boolean hasFn (String fterm) {
		String[] strs = fterm.substring(0, fterm.length() - 2).split("(");
		String fnName = strs[0];
		String field = strs[1];
		if (fnName.equals("max"))
			this.aggfn = new MaxFn(field);
		else
			this.aggfn = new CountFn(field);
		return this.aggfn == null;
	}

	public Collection<String> getFields (String fnName) {
		return fnFlds.get(fnName);
	}
	
	/**
	 * 
	 */
	private void initFnFlds () {
		this.fnFlds = new HashMap<String, ArrayList<String>>();
		// input self defined functions
		fnFlds.put("distance", (ArrayList<String>) Arrays.asList("latitude", "longitude"));
	}
	
	/**
	 * 
	 * @param fnName
	 * @return
	 */
	public boolean isFn (String fnName) {
		if (this.fnFlds.containsKey(fnName)) {
			this.fn = fnName;
			return true;
		}
		return false;
	}

	public void setFldVal (Constant val) {
		this.fldvals.add(val);
	}
	/**
	 * 
	 * @return
	 */
	public Constant excFn () {
		// check the keyword of function
		if (this.fn == "distance()")
			return excDistance();
		// if None return null
		return null;
	}
	/**
	 * 
	 * @return
	 */
	private Constant excDistance () {
		float lg = (Float) this.fldvals.get(0).asJavaVal();
		float lt =  (Float) this.fldvals.get(1).asJavaVal();

		return new FloatConstant((float) Math.sqrt(
		                         (lg - SELF_LOG) * (lg - SELF_LOG) +
		                         (lt - SELF_LAT) * (lt - SELF_LAT)));
	}
}
