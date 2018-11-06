package simpledb.index.linearhash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import simpledb.server.SimpleDB;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Sixing Yan
 */
public class LinearHashIndex {
	// non-clustering !
	// bucket is part of the index files, each bucket is a file of indexing 
	public static final int MAX_NAME = 16;
	public static final int DFLT_ROUND = 1;
	public static final int DFLT_SIZE = 4;
	public static final int DFLT_COUNT = 4;
	public static final int DFLT_SPLIT = 0;
	public static final int RDNUM_MAX = 100;
	
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
		this.sch = sch;
		this.tx = tx;
		initLinearHash();
	}

	public void initLinearHash() {
		// 1. deal with function
		String functbl = this.idxname + "func";
		Schema funcsch = new Schema();
		funcsch.addStringField("funcname", MAX_NAME);
		funcsch.addIntField("round");
		funcsch.addIntField("size");
		funcsch.addIntField("count");
		funcsch.addIntField("split");

		if (tx.size("lnrhshcat") == 0) {
			// create linear-hash file
			SimpleDB.mdMgr().createTable("lnrhshcat", funcsch, this.tx); // tablemgr
			
			// insert a record about this function
			this.funcTi = new TableInfo(functbl, funcsch);
			createFunction(funcTi);
		}
		else {
			// open linear-hash file
			this.funcTi = new TableInfo(functbl, funcsch);
    
			// get the related record
			RecordFile fcatfile = new RecordFile(this.funcTi, tx);
			Boolean flag = false;
			while (fcatfile.next())
				if(fcatfile.getString("funcname").equals(this.idxname + "func")) {
					flag = true;
					this.size = fcatfile.getInt("size");
					this.count = fcatfile.getInt("count");
					this.split = fcatfile.getInt("split");
					this.round = fcatfile.getInt("round");
					break;
				}	
				if (flag != true)
					createFunction(funcTi);
		}
	}

	public void createFunction(TableInfo funcTi) {
      	// insert one record into tblcat
      	RecordFile fcatfile = new RecordFile(funcTi, tx);
      	fcatfile.insert();
      	fcatfile.setString("funcname", funcTi.fileName());
      	fcatfile.setInt("round", DFLT_ROUND);
      	fcatfile.setInt("size", DFLT_SIZE);
      	fcatfile.setInt("count", DFLT_COUNT);
      	fcatfile.setInt("split", DFLT_SPLIT);
      	fcatfile.close();

      	this.funcRid = fcatfile.currentRid();

      	// init members
      	this.count = DFLT_COUNT;
      	this.size = DFLT_SIZE;
      	this.round = DFLT_ROUND;
      	this.split = DFLT_SPLIT;

      	//where to initial the bucket?
      	for (int bkt=0; bkt < this.count; bkt++)
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
			// 做这一步其实为了防止hash collision
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

		if ((rdnum + 1) >= RDNUM_MAX)
			splitBucket(val);
	}

	/**
	 * Works for splitBucket()
	 * Inserts a new record into the table scan for the bucket.
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
		while(next())
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
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	private int linearHash() {
		int key = this.searchkey.hashCode();
		int bktnum = hash(key, this.round);
		if (bktnum < this.split) 
			bktnum = hash(key, this.round + 1);
		return bktnum;
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	private int hash(int key, int round) {
		return key % (this.count * round);
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	private void splitBucket(Constant val) {
		// new a bucket and open its scan
		String tblname = this.idxname + (this.count + 1);
		SimpleDB.mdMgr().createTable(tblname, this.sch, this.tx); // tablemgr
		TableInfo ti = new TableInfo(tblname, sch); // this will open a bucket
		TableScan newts = new TableScan(ti, this.tx);

		// move to the target old bucket
		beforeFirst(val);
		while(next()){
			int bkt = linearHash();
			if (bkt >= this.size) {
				RID rid = getDataRid();
				insert(ts.getVal("dataval"), rid, newts); // rewrite function
				delete(ts.getVal("dataval"), rid);
			}
		}

		// update the parameter
		this.count ++;
		this.split ++;
		if(this.split >= this.size){  //分裂点移动了一轮就更换新的哈希函数  
			this.round ++;  
			this.size = this.size * 2;  
			this.split = 0;
		}

		// write the new parameter into dist
		updateFunction();
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
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
}