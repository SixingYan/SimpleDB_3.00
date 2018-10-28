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
  
	private int splitPoint = 0; //分裂点    
	private int hashSize = 4; //哈希表的初始大小  
	private int hashVal = 4; //哈希大小的记录值  
	private int round = 0;  //分裂轮数  	  
	private List<Map<Integer, String>> hash; //模拟哈希表  
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
		initHash();
	}
	/**
	 * If there is some pre-configure, update the related members.
	 * Check the 'idxcat' file to see whether there is an index with idxname.
	 *  
	 */
	public void initHash(
		int splitPoint, int hashVal, int round, ) {
		this.splitPoint = splitPoint;
		this.hashVal = hashVal;
		this.round = round;
		this.hash = new ArrayList<Map<Integer, String>>();
		for (int i = 0; i < this.hashSize; i++)
			hash.add(new HashMap<Integer, String>()); //向哈希表中初始化桶


		this.idxname;

	}

	private void initHash() {
		this.hash = new ArrayList<Map<Integer, String>>();
		for (int i = 0; i < this.hashSize; i++)
			hash.add(new HashMap<Integer, String>()); //向哈希表中初始化桶
	}
	/**
	 * Hash function to obtain index value
	 * 
	 */
	private int hashIndex(int key, int round) {
		return key % (this.hashVal * round)
	}

	private void splitHash() {
		Map<Integer, String> oldMap = this.hash.get(this.splitPoint);   //旧桶  
		Map<Integer, String> newMap = new HashMap<Integer, String>(); //分裂产生的新桶  
		
		Integer[] keyList = oldMap.keySet().toArray(new Integer[0]);  
		for (int i = 0; i < keyList.length; i++) {  //准备移动一半的数据到新桶  
			int key = keyList[i].intValue();  
			int index = hashIndex(key, this.round + 1);  
			if (index >= hashSize) { 
				newMap.put(key, oldMap.get(key));  
				oldMap.remove(key);
			}  
		}  
		this.hash.add(newMap);  //将新桶放入哈希表  

		this.hashSize ++;  //哈希表长度增加  
		this.splitPoint ++;  //分裂点移动  
		if(this.splitPoint >= this.hashVal){  //分裂点移动了一轮就更换新的哈希函数  
			this.round ++;  
			this.hashVal = this.hashVal * 2;  
			this.splitPoint = 0;  
		}  
	}

	/**
	 * 
	 * 
	 */

	private int getBucketIndex(int key){
		int idx = hashIndex(key, this.round);
		if (idx < this.splitPoint) 
			idx = hashIndex(key, this.round + 1);
		return hash.get().get(key)
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
	// 
	public void beforeFirst(Constant searchkey) { 
		close(); // end up the scan on the last file
		this.searchkey = searchkey;
		int bucket = getBucketIndex(searchkey.hashCode()) //  这才是索引出现的地方
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch); // this will open a bucket
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
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}

	public void indexInsert() {

	}

	public void insert(Constant val, RID rid) {




		int idx = hashIndex(key, this.round);     
        if(idx < this.splitPoint){  
            index = hashIndex(key, this.round + 1);  
        }
        Map<Integer, String> map = this.hash.get(idx);  
        if (map.size() < this.NUM_BUCKETS) {   //判断当前桶是否满了  
            map.put(key, value);  
        } 
        else {  
            map.put(key, value);    
            splitHash();              //满了就进行分裂  
        }  

        beforeFirst(val);

        indexInsert(rid.blockNumber());
        splitHash()

		this.ts.insert();
		this.ts.setInt("block", rid.blockNumber());
		this.ts.setInt("id", rid.id());
		this.ts.setVal("dataval", val);


		beforeFirst(val);
		ts.insert();
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
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}
}



hashcat.
	private void initHash(){
		// 读二进制文件 
		int hashNumber = (this.tblname+this.idxname+this.fldname).hashCode();
		hashFilename = "hashcat" + hashNumber;
		IndexFileMgr idxfMgr = New IndexFileMgr(hashFilename);
		if (idxfMgr.hasIndex(hashFilename)) {
			Map idxFile = idxfMgr.get();
			idxFile.get("");
			this.splitPoint = idxFile.get("config").get("splitPoint")
			this.hash = idxFile.get("index")

		}
		else { // 每次更新操作结束就写一次
			// 初始化的时候也要做一次
			this.hash = new ArrayList<Map<Integer, String>>();
			for (int i = 0; i < this.hashSize; i++)
				hash.add(new HashMap<Integer, String>()); //向哈希表中初始化桶
			Map config = new Map<String, Integer>();
			config.put("splitPoint",this.);
			config.put("", this.);

			idxfMgr.put(config, this.hash);
		}

	}













