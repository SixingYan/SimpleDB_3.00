package simpledb.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.metadata.IndexInfo;
import simpledb.parse.QueryData;
import simpledb.query.Constant;
import simpledb.query.Term;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
/**
 *
 * @author Sixing Yan
 */
public class IndexFinder {
	private QueryData data;
	private Transaction tx;
	private String idxname;
	private ArrayList<Index> idxlist = new ArrayList<Index>();
	private Map<String, Integer> costmap = new HashMap<String, Integer>();
	private Map<String, Constant> keymap = new HashMap<String, Constant>();
	private Map<String, IndexInfo> idximap = new HashMap<String, IndexInfo>();

	public static String RT_TBL = "businesslocation";

	public IndexFinder (QueryData data, Transaction tx) {
		this.data = data;
	}

	/**
	 * Obtain index info of each field in "where" and each table in "from"
	 */
	public void obtainIndex() {
		for (String tblname : data.tables()) {
			Map<String, IndexInfo> iiMap = SimpleDB.mdMgr().getIndexInfo(tblname, this.tx);
			if (iiMap.isEmpty()) continue;
			for (String fldname : iiMap.keySet()) { // field with index on it
				IndexInfo ii = iiMap.get(fldname);

				// search for terms like F1 * c, e.g., F1 > c
				Constant cst = data.pred().operatesWithConstant(fldname);
				if (cst != null & iiMap.get(fldname) != null)
					addConstant(ii, cst);

				// search for terms like F1 * F2, e.g., F1 = F2
				String fld = data.pred().operatesWithField(fldname);
				if (fld != null)
					addField(ii, fldname, fld);
			}
		}
	}

	public boolean toCallRTree() {
		if (!data.pred().fns.isEmpty())
			for (String fn : data.pred().fns)
				if (fn == Term.DIST_FN)
					return true;
		return false;
	}

	public boolean obtainRTreeIndex() {
		if (data.tables().contains(RT_TBL)) {
			Map<String, IndexInfo> iiMap = SimpleDB.mdMgr().getIndexInfo(RT_TBL, this.tx);
			if (iiMap.containsKey("rtindex"))
				idximap.put("indexname", iiMap.get("indexname"));
			return true;
		}
		return false;
	}

	/**
	 *
	 * @param ii
	 * @param cst
	 */
	private void addConstant(IndexInfo ii, Constant cst) {
		// 1. record the index-searchkey, e.g., id=3 where searchkey is 3
		String idxname = ii.idxname;
		keymap.put(idxname, cst);

		// 2. record the index-cost, the time cost of using this index
		//IndexContent idxcnt = new IndexContent(ii, tx);
		//costmap.put(idxname, idxcnt.getIndexSearchCost());

		// 3. record the index-indexinfo
		idximap.put(idxname, ii);
	}

	/**
	 *
	 * @param ii
	 * @param fldname
	 * @param fld
	 */
	private void addField(IndexInfo ii, String fldname, String fld) {
		// left blank
		
	}

	/**
	 *
	 */
	private void getMinCostIndex() {
		int minCost = -1;
		// get the minimum cost of using an index
		for (String idxname : costmap.keySet())
			if (costmap.get(idxname) < minCost)
				this.idxname = idxname;

		// compare the index cost vs. scan cost
		int scanCost = 100000000; // false value !
		if (minCost > scanCost)
			this.idxname = null;
	}

// methods called by palnner.IndexQueryPlanner
	/**
	 *
	 * @return
	 */
	public boolean hasIndexInfo () {
		// try to get rtree index first
		if (data.tables().contains(RT_TBL) & toCallRTree())
			if (!obtainRTreeIndex())
				obtainIndex();

		//if (keymap.isEmpty() != false)
		//	getMinCostIndex();
		// Choose the first one
		for (String idxname : keymap.keySet()) {
			this.idxname = idxname;
			break;
		}

		return this.idxname != null ? true : false;
	}

	/**
	 *
	 * @return
	 */
	public IndexInfo getIndexInfo () {
		return idximap.get(this.idxname);
	}

	/**
	 *
	 * @return
	 */
	public Constant getSearchKey () {
		return keymap.get(this.idxname);
	}
}












