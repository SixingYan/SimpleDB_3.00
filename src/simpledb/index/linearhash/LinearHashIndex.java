package simpledb.index.linearhash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.server.SimpleDB;
/**
 * A linear hash implementation of the Index interface.
 * A default number of buckets is allocated at the beginning (currently, 25),
 * and each bucket is implemented as a file of index records with a fixed number (currently, 100).
 * @author Sixing Yan
 */
public class LinearHashIndex implements Index {
	// non-clustering !
	// bucket is part of the index files, each bucket is a file of indexing
	public static int DFLT_COUNT = 25;
	public static int DFLT_SIZE = 25;
	public static int DFLT_ROUND = 1;
	public static int DFLT_SPLIT = 0;
	public static int DFLT_CPCT = 100; // the capacity of each bucket
	public static int MAX_NAME = 20;
	
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;

	private int round; // 第几回合
	private int split; // 分裂点坐标
	private int size; // function的最大bucket数量
	private int count; // 目前有多少个bucket
	private RID funcRid;
	private TableInfo funcTi;

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public LinearHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch; // 这个似乎是固定的，在IndexInfo的schema()这个函数获得
		this.tx = tx;
		initLinearHash();
	}

	/**
	 * initial the linear hash funcion,
	 * get the current parameter if this function exists,
	 * or register a new funciton into the function file.
	 */
	public void initLinearHash() {
		String functbl = this.idxname + "func";
		Schema funcsch = new Schema();
		funcsch.addStringField("funcname", MAX_NAME);
		funcsch.addIntField("round");
		funcsch.addIntField("size");
		funcsch.addIntField("count");
		funcsch.addIntField("split");

		if (tx.size("lnrhshcat") == 0) // if the function file no exists
			// create linear-hash file
			SimpleDB.mdMgr().createTable("lnrhshcat", funcsch, this.tx);// tablemgr

		// open linear-hash file
		this.funcTi = new TableInfo(functbl, funcsch);

		// get the related record
		RecordFile fcatfile = new RecordFile(this.funcTi, tx);
		Boolean flag = false;
		while (fcatfile.next())
			if (fcatfile.getString("funcname").equals(this.funcTi.fileName())) {
				flag = true;
				this.size = fcatfile.getInt("size");
				this.count = fcatfile.getInt("count");
				this.split = fcatfile.getInt("split");
				this.round = fcatfile.getInt("round");
				break;
			}
		if (flag != true) // if there no exist the related record
			createFunction(funcTi);
	}

	/**
	 * register a linear hash fucntion,
	 * insert a record of its parameters into the linear-hash-function file.
	 * create the default buckets for this function.
	 */
	public void createFunction(TableInfo funcTi) {
		// a record of parameter into tblcat
		RecordFile fcatfile = new RecordFile(funcTi, tx);
		fcatfile.insert();
		fcatfile.setString("funcname", funcTi.fileName());
		fcatfile.setInt("round", DFLT_ROUND);
		fcatfile.setInt("size", DFLT_SIZE);
		fcatfile.setInt("count", DFLT_COUNT);
		fcatfile.setInt("split", DFLT_SPLIT);
		fcatfile.close();

		// record the information of current function
		this.funcRid = fcatfile.currentRid();
		this.count = DFLT_COUNT;
		this.size = DFLT_SIZE;
		this.round = DFLT_ROUND;
		this.split = DFLT_SPLIT;

		//initial default buckets
		for (int bkt = 0; bkt < this.count; bkt++)
			SimpleDB.mdMgr().createTable(this.idxname + bkt, this.sch, this.tx); // tablemgr
	}

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close(); // end up the scan on the last file
		this.searchkey = searchkey;
		int bucket = linearHash();
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch); // this will open a bucket
		this.ts = new TableScan(ti, tx);
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next())
			// avoid hash collision, different dataval but the same hashed key
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		int rdnum = ts.insert(); // already modify RecordFile.insert(), make it return how many record it pass (integer)
		this.ts.setInt("block", rid.blockNumber());
		this.ts.setInt("id", rid.id());
		this.ts.setVal("dataval", val);

		if ((rdnum + 1) >= DFLT_CPCT) // reach the maximum bucket capacity 
			splitBucket(val);
	}

	/**
	 * Inserts a new record into the given table scan for the bucket,
	 * only works for splitBucket() when transfer records to a new bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid, TableScan ts) {
		beforeFirst(val);
		ts.insert(); // already modify RecordFile.insert(), make it return how many record it pass (integer)
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while (next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Calculate the bucket number by linear hash function,
	 * cooperate with hash().
	 * @return the bucket number
	 */
	private int linearHash() {
		int key = this.searchkey.hashCode();
		int bktnum = hash(key, this.round);
		if (bktnum < this.split)
			bktnum = hash(key, this.round + 1);
		return bktnum;
	}

	/**
	 * Calculate the hashed key.
	 * @param key the value to be hashed
	 * @param round the current round number
	 */
	private int hash(int key, int round) {
		return key % (this.count * round);
	}

	/**
	 * Split the bucket when it is full after adding a new item.
	 * @see simpledb.index.btree.BTreePage#split()
	 */
	private void splitBucket(Constant val) {
		// 1. new a bucket and open its scan
		String tblname = this.idxname + (this.count + 1);
		SimpleDB.mdMgr().createTable(tblname, this.sch, this.tx); // tablemgr
		TableInfo ti = new TableInfo(tblname, sch); // this will open a bucket
		TableScan newts = new TableScan(ti, this.tx);
		// 2. move to the target old bucket
		beforeFirst(val);
		while (next()) {
			int bkt = linearHash();
			if (bkt >= this.size) {
				RID rid = getDataRid();
				insert(ts.getVal("dataval"), rid, newts); // rewrite function
				delete(ts.getVal("dataval"), rid);
			}
		}
		// 3. update the parameter locally
		this.count ++;
		this.split ++;
		if (this.split >= this.size) { //分裂点移动了一轮就更换新的哈希函数
			this.round ++;
			this.size = this.size * 2;
			this.split = 0;
		}
		// 4. write the new parameter into record file
		updateFunction();
	}

	/**
	 * Update the parameters of linear hash function,
	 * including round/size/count/split parameters.
	 * First, move to the record of current function,
	 * then set the value by RecordFile
	 */
	private void updateFunction() {
		RecordFile fcatfile = new RecordFile(this.funcTi, tx);
		fcatfile.moveToRid(this.funcRid);

		fcatfile.setInt("round", this.round);
		fcatfile.setInt("size", this.size);
		fcatfile.setInt("count", this.count);
		fcatfile.setInt("split", this.split);
		fcatfile.close();
	}
	
	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records (not used here)
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return LinearHashIndex.DFLT_CPCT/2; //assume every bucket is used a half
	}
}