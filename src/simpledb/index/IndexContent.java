package simpledb.index;

import simpledb.index.hash.HashIndex;
import simpledb.index.linearhash.LinearHashIndex;
//import simpledb.index.extendablehash.ExtendableHashIndex; // ongoing implement
import simpledb.index.btree.BTreeIndex;
//import simpledb.index.kdtree.KdTreeIndex; // ongoing implement
import simpledb.index.rtree.RTreeIndex;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The content of every avaible index in this project.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the schema of the index records.
 * Its methods are essentially the same as those of Plan.
 * @author Sixing Yan
 */
public class IndexContent {
	public static String SEPERATOR = "@";
	public static int IDXTYPE_IDX = 2;
	public static String LINEAR_HASH = "lhsh";
	public static String RTREE = "rtr";
	public static String BTREE = "btr";
	
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private String idxtype;
	
	public IndexContent(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
	}

	public Index initIndex() {
		Index idx;
		getIndexType();
		
		if (this.idxtype.equals(LINEAR_HASH))
			idx = new LinearHashIndex(idxname, sch, tx);
		//else if (this.idxtype.equals(LINEAR_HASH))
		//	idx = new ExtendableHashIndex();
		//else if (this.idxtype.equals(LINEAR_HASH))
		//	idx = new KdTreeIndex();
		else if (this.idxtype.equals(RTREE))
			idx = new RTreeIndex(idxname, sch, tx);
		else if (this.idxtype.equals(BTREE))
			idx = new BTreeIndex(idxname, sch, tx);
		else
			idx = new HashIndex(idxname, sch, tx);
		return idx;
	}

	private void getIndexType() {
		String[] parts = this.idxname.split(SEPERATOR);
		this.idxtype = parts[IDXTYPE_IDX];
	}	
}