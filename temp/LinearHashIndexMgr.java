public class LinearHashIndexMgr {
	private String tblname;
	private String idxname;
	private Transaction tx;
	private int count;
	public int split; // use getset to read/write
	public int round; // use getset to read/write

	//private TableInfo hcatInfo, hfcatInfo;

	// param isNew detemine whether there is a index 
	public LinearHashMgr(String tblname, String idxname, Transaction tx) {
		this.tblname = tblname;
		this.idxname = idxname;
		this.tx = tx;
		init();



		if (isnew) {
			if (LinearHashFileCatIsNew()) { // create linearHashCat first
				Schema lhcatSchema = new Schema();
	  			lhcatSchema.addStringField("indexname", MAX_NAME);
	  			lhcatSchema.addIntField("split");
	  			lhcatSchema.addIntField("round");
	  			lhcatSchema.addIntField("count");
	  			this.lhcatInfo = new TableInfo("lhashcat", lhcatSchema);
			}
			
			createLinearHash();
   		}
   		else {
   			// get the record

   		}
   	}

   	private void init() {
   		this.hti = tblmgr.getTableInfo("", tx);
   		RecordFile rf = new RecordFile(this.hti, tx);
      	while (rf.next())
         	if (rf.getString("tablename").equals(this.tblname) & rf.getString("indexname").equals(this.idxname)) {
         		this.split = rf.getInt("indexname");
         		this.round = rf.getInt("round");
         		this.count = rf.getInt("count");
         		break;
      		}
      	rf.close();
      	

      	if (this.count == 0) {
      		String fcatno = String.valueOf((this.tblname+this.idxname).hashCode());
      		Schema sch = new Schema();
      		sch.addIntField("blocknumber");
      		for (int i = 0; i < DEFAULT_LHASH_TBL_SIZE; i ++) {
      			tblmgr.createTable("lhidxcat" + fcatno + "_" + i, sch, tx);
      		}
      		this.count = DEFAULT_LHASH_TBL_SIZE;
      	}

   	}

   private void initLinearHashFile () {
   String fcatno = String.valueOf((this.tblname+this.idxname).hashCode());
      		Schema sch = new Schema();
      		sch.addIntField("blocknumber");
      		for (int i = 0; i < DEFAULT_LHASH_TBL_SIZE; i ++) {
      			tblmgr.createTable("lhidxcat" + fcatno + "_" + i, sch, tx);
      		}
   }


   	// this isnew work for linearHashCat file
   	
   	public Boolean LinearHashCatIsNew () {
		Map<String,IndexInfo> idxMap = SimpleDB.mdMgr().getIndexInfo(this.tblname, this.tx);
		if (idxMap.containsKey(this.idxname))
			return true;
		else
			return false;
	}
	public Boolean LinearHashFileCatIsNew () {

   	}

   	public void createLinearHash() {	
   		RecordFile rf = new RecordFile(this.lhcatInfo, tx);
      	rf.insert();
      	rf.setString("indexname", idxname);
      	rf.setInt("split", DEFAULT_SPLIT);
      	rf.setInt("round", DEFAULT_ROUND);
      	rf.close();

      	initLinearHashFile(indexname.hashCode());
	}

	public void initLinearHashFile (int lhfcatno) {
		Schema lhfcatSchema = new Schema();
   		for (int i = 0; i < INIT_HASH_TBL_SIZE; i++) {
   			lhfcatSchema.addIntField("blocknumber");
	  		this.lhfcatInfo = new TableInfo("lhashfcat" + lhfcatno + "_" + i, lhfcatSchema);
	  	}
	}

	public getLinearHashIndex() {

	}

	public put () {

	}

	public createLinearHashFile () {

	}

}