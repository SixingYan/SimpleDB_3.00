public class LinearHashIndexMgr {
	private TableInfo lhti; 
	private String tblname;
	private String idxname;
	private Transaction tx;
	private int size;
	private int count;
	private int split; 
	private int round;
	private String lhfname;

	public LinearHashMgr(String idxname, Transaction tx) {
		this.idxname = idxname;
		this.tx = tx;
		this.lhfname = "lhidxcat" + idxname.hashCode();
		init();
   }

   	private void init() {
   		this.lhti = tblmgr.getTableInfo("lhashcat", tx);
   		RecordFile rf = new RecordFile(this.lhti, tx);
      	while (rf.next())
         	if (rf.getString("indexname").equals(this.idxname)) {
         		this.split = rf.getInt("split");
         		this.round = rf.getInt("round");
         		this.count = rf.getInt("count");
         		this.size = rf.getInt("size");
         		this.rid = rf.currentRid();
         		break;
      		}
      	rf.close();
   	}

   	public void insertBucket(int bktno, ArrayList<Integer> bucket) {
   		// 1. create new bucket
   		createBucket(bktno);
   		// 2. insert data into bucket
   		TableInfo lhfti = SimpleDB.mdMgr().getTableInfo(getBucketName(bktno), tx);
   		RecordFile rf;
   		for (int i = 0; i < bucket.size(); i ++) {
   			rf = new RecordFile(lhfti, tx);
      		rf.insert();
      		rf.setInt("blocknumber", bucket.get(i));
      		rf.close();
   		}
   	}

   	public void createBucket(int bktno) {
   		Schema sch = new Schema();
   		sch.addIntField("blocknumber");
   		SimpleDB.mdMgr().createTable(getBucketName(bktno), sch, this.tx);
   	}

   	public void updateBucket(int bktno, ArrayList<Integer> bucket) {
   		deleteBucket(bktno);
   		insertBucket(bktno, bktname);
   	}

   	public void deleteBucket(int bktno) {
   		SimpleDB.mdMgr().deleteTable(getBucketName(bktno), tx);
   	}

   	private String getBucketName(int bktno) {
   		return this.lhfname + "_" + bktno;
   	}

   	public void updateConfig (int count, int round, int size, int split) {
   		RecordFile rf = new RecordFile(this.lhti, tx);
   		rf.moveToRid(this.rid);
   		rf.insert();
      	rf.setInt("count", count);
      	rf.setInt("round", round);
      	rf.setInt("size", size);
      	rf.setInt("split", split);
      	rf.close();
   	}

	public int getSplit () {
		return this.split;
	}
	public int getCount () {
		return this.count;
	}
	public int getRound () {
		return this.round;
	}
	public int getSize () {
		return this.size;
	}
}