package simpledb.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.metadata.IndexInfo;
import simpledb.parse.QueryData;
import simpledb.query.Constant;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class IndexFinder {
	private QueryData data;
	private Transaction tx;
	private ArrayList<Index> idxlist = new ArrayList<Index>();
	private Map<String, Integer> costmap = new HashMap<String, Integer>();
	private Map<String, Constant> keymap = new HashMap<String, Constant>();
	
	public IndexFinder (QueryData data, Transaction tx) {
		this.data = data;
		obtainIndex();
	}
	
	public void obtainIndex() {
		for (String tblname : data.tables()) {
			Map<String, IndexInfo> iiMap = SimpleDB.mdMgr().getIndexInfo(tblname, this.tx);
			if (iiMap.isEmpty()) continue;
			for (String fldname: iiMap.keySet()) {
				// search for terms like F1 * c, e.g., F1 > c
				Constant cst = data.pred().operatesWithConstant(fldname);
				if (cst != null & iiMap.get(fldname) != null)
					addConstant(tblname, fldname, cst);
				// search for terms like F1 * F2, e.g., F1 = F2
				String fld = data.pred().operatesWithField(fldname);
				if (fld != null)
					addField(tblname, fldname, fld);
			}
		}
	}
	
	private void addConstant(IndexInfo ii, Constant cst) {
		String idxname = ii.idxname;
		keymap.put(idxname, cst);
		
		IndexContent idxcnt = new IndexContent();
		idxc = idxcnt.getIndexClass();
		
		costmap.put(idxname, idxcnt.getSearchCost());
		
		
	}
	
	private void addField(String tblname, String fldname, String fld) {
		
		costmap.put(idxname, searchcost);
	}
	
	public Index getIndexInfo() {
		// calculate the minimum cost
	}
	
	public Constant getSearchKey() {
		
	}
}
