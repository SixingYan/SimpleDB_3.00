public class LinearHashIndexMgr {
	private String tblname;
	private String idxname;
	private Transaction tx;
	private int count;
	private int split; 
	private int round; 
	private String lhfname;


	// 是否需要move to rid 然后才开始 insert 嘛
	//private TableInfo hcatInfo, hfcatInfo;

	// param isNew detemine whether there is a index 
	public LinearHashMgr(String tblname, String idxname, Transaction tx) {
		this.tblname = tblname;
		this.idxname = idxname;
		this.tx = tx;
		this.lhfname = "lhidxcat" + (tblname+idxname).hashCode();
		init();
   	}

   	private void init() {
   		this.hti = tblmgr.getTableInfo("", tx);
   		RecordFile rf = new RecordFile(this.hti, tx);
      	while (rf.next())
         	if (rf.getString("tablename").equals(this.tblname) & rf.getString("indexname").equals(this.idxname)) {
         		this.split = rf.getInt("split");
         		this.round = rf.getInt("round");
         		this.count = rf.getInt("count");
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

   	public void updateConfig (int round, int size, int split) {

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

	public void setCount () {

	}












}