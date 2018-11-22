package simpledb.index;

import simpledb.index.hash.HashIndex;
import simpledb.index.linearhash.LinearHashIndex;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.rtree.RTreeIndex;
import simpledb.metadata.IndexInfo;
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
	public static String HASH = "hsh";
	public static String LINEAR_HASH = "lhsh";
	public static String EXTENT_HASH = "ehsh";
	public static String RTREE = "rtr";
	public static String BTREE = "btr";
	public static String KDTREE = "kdtr";
	public static int NOT_USED = -1;

	private String idxname;
	private Schema sch;
	private Transaction tx;
	private String idxtype;
	private IndexInfo ii;

	public IndexContent(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		getIndexType();
	}
	public IndexContent(IndexInfo ii, Transaction tx) {
		this.ii = ii;
		getIndexType();
	}

	/**
	 * @return search cost
	 */
	public int getIndexSearchCost() {
		int searchcost;
		if (this.idxtype.equals(LINEAR_HASH))
			searchcost = LinearHashIndex.searchCost(NOT_USED, NOT_USED);
		else if (this.idxtype.equals(RTREE))
			searchcost = -1;//RTreeIndex.searchCost(NOT_USED, NOT_USED);
		else if (this.idxtype.equals(BTREE))
			searchcost = BTreeIndex.searchCost(tx.size(idxname + "dir" + ".tbl"), tx.size(idxname + "leaf" + ".tbl"));
		else {
			int totalBlock = 0;
			for (int i = 0; i < HashIndex.NUM_BUCKETS; i++)
				totalBlock = totalBlock + tx.size(idxname + i);
			searchcost = HashIndex.searchCost(totalBlock, NOT_USED);
		}

		return searchcost;
	}

	/**
	 *
	 * @return instance of index
	 */
	public Index getIndex() {
		Index idx;
		if (this.idxtype.equals(LINEAR_HASH))
			idx = new LinearHashIndex(idxname, sch, tx);
		else if (this.idxtype.equals(RTREE))
			idx = new RTreeIndex(idxname, sch, tx);
		else if (this.idxtype.equals(BTREE))
			idx = new BTreeIndex(idxname, sch, tx);
		else
			idx = new HashIndex(idxname, sch, tx);
		return idx;
	}

	/**
	 *
	 */
	private void getIndexType() {
		String[] parts = this.idxname.split(SEPERATOR);
		this.idxtype = parts[IDXTYPE_IDX];
	}
}