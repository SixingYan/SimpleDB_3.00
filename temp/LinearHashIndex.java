package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Sixing Yan
 */




public class LinearHashIndex implements Index {
	public static int NUM_BUCKETS = 100;  //桶的容量  
  	private LinearHashIndexMgr lhiMgr;
  	private int bktIndex;
	private int split; //分裂点    
	private int count; //哈希表的初始大小  
	private int size; //哈希大小的记录值 4   
	private int round;  //分裂轮数 start by 1  	  
	private List<List<Integer>> hashList; //模拟哈希表  
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;

	/**
	 * Opens a linear hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */

	public LinearHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		this.lhiMgr = new LinearHashIndexMgr(idxname, tx);
		initHash();
	}

	/**
	 * If there is some pre-configure, update the related members.
	 * Check the 'idxcat' file to see whether there is an index with idxname.
	 *  
	 */
	public void initHash() {
		this.split = this.lhiMgr.getSplit();
		this.round = this.lhiMgr.getRound();
		this.count = this.lhiMgr.getCount();
		this.size = this.lhiMgr.getSize();
		if (this.count == 0) 
			initHashIndexBucket();
		else 
			for (int i = 0; i < this.count; i ++)
				this.hashList.add(this.lhiMgr.getBucket(i));
	}

	/**
	 * Hash function to obtain index value
	 * 
	 */
	public initHashIndexBucket() {
		//
		this.hashList = new ArrayList<Integer>();
		//
		this.count = INIT_LHASH_TBL_SIZE;
		ArrayList bucket;
		for (int i = 0; i < this.count; i++){
			bucket = new ArrayList<Integer>();
			this.hashList.add(bucket); //向哈希表中初始化桶
			this.lhiMgr.insertBucket(i,bucket);
		}
		this.lhiMgr.setCount(INIT_LHASH_TBL_SIZE);
	}

	/**
	 * Hash function to obtain index value
	 * 
	 */
	private int hashIndex(int key, int round) {
		return key % (this.size * round);
	}

	/**
	 * Hash function to obtain index value
	 * 
	 */
	private void splitBucket() {
		ArrayList<Integer> oldBucket = this.hash.get(this.split);   //旧桶  
		ArrayList<Integer> newBucket = new HashMap<Integer, String>(); //分裂产生的新桶  
		
		int blkno;
		for (int i = 0; i < oldBucket.size(); i++) {  //准备移动一半的数据到新桶  
			oldBucket.get(i);
			blkno = oldBucket.get(i);
			this.bktIndex = hashIndex(blkno, this.round + 1);
			if (this.bktIndex >= this.count) { 
				newBucket.add(blkno);  
				oldBucket.removeAt(i);
			}
			this.lhiMgr.updateBlock(i, oldBucket);
		}
		
		this.hash.add(newBucket);  //将新桶放入哈希表  
		this.count ++;  //哈希表长度增加
		this.lhiMgr.insertBucket(this.count, newBucket); // 将新桶插入本地

		this.split ++;  //分裂点移动  
		
		if(this.split >= this.size){  //分裂点移动了一轮就更换新的哈希函数  
			this.round ++;  
			this.size = this.size * 2;  
			this.split = 0;
		}
		updateConfig();
	}
	private void updateConfig() {
		this.lhiMgr.updateConfig(this.count, this.round, this.size, this.split);
	}
	/**
	 * 
	 * 
	 */
	private int getBucketIndex(int key){
		this.bktIndex = hashIndex(key, this.round);
		if (this.bktIndex < this.split) 
			this.bktIndex = hashIndex(key, this.round + 1);
		return hash.get().get(key);
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
		int bucket = getBucketIndex(searchkey.hashCode()) //  这才是索引出现的地方
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, this.sch); // this will open a bucket
		ts = new TableScan(ti, tx);
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
        this.hashList.get(this.bktIndex).add(rid.blockNumber()); 
        if (bucket.size() > NUM_BUCKETS)  //判断当前桶是否满了   
            splitBucket();              //满了就进行分裂  

		this.ts.insert();
		this.ts.setInt("block", rid.blockNumber());
		this.ts.setInt("id", rid.id());
		this.ts.setVal("dataval", val);
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		// non-update
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
}