
import simpledb.index.hash.HashIndex;
import simpledb.index.linearhash.LinearHashIndex;
import simpledb.index.extendablehash.ExtendableHashIndex;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.kdtree.KdTreeIndex;
import simpledb.index.rtree.RTreeIndex;

/**
 * The content of every avaible index in this project.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the schema of the index records.
 * Its methods are essentially the same as those of Plan.
 * @author Sixing Yan
 */
public class IndexContent {
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private String idxtype;
	
	public IndexContent(String idxname, Schema sch, tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
	}

	public Index initIndex() {
		Index idx;
		getIndexType();
		if (this.idxtype.equals(LINEAR_HASH))
			idx = new LinearHashIndex();
		else if (this.idxtype.equals(LINEAR_HASH))
			idx = new ExtendableHashIndex();
		else if (this.idxtype.equals(LINEAR_HASH))
			idx = new KdTreeIndex();
		else if (this.idxtype.equals(LINEAR_HASH))
			idx = new RTreeIndex();
		else if (this.idxtype.equals(LINEAR_HASH))
			idx = new BTreeIndex();
		else
			idx = new HashIndex();
		return idx;
	}

	private void getIndexType() {
		String[] parts = this.idxname.split(SEPERATOR);
		this.idxtype = parts[IDXTYPE_IDX];
	}