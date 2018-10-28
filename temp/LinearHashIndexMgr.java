public class LinearHashIndexMgr {
	
	private TableInfo hcatInfo, hfcatInfo;

	// param isNew detemine whether there is a index 
	public LinearHashMgr(String idxname, Transaction tx) {
		this.idxname = idxname;
		this.tx = tx;
		if (LinearHashCatIsNew()) {
			if (LinearHashFileCatIsNew()) { // create linearHashCat first
				Schema lhcatSchema = new Schema();
	  			lhcatSchema.addStringField("indexname", MAX_NAME);
	  			lhcatSchema.addIntField("split");
	  			lhcatSchema.addIntField("round");
	  			this.lhcatInfo = new TableInfo("lhashcat", lhcatSchema);
			}
			
			createLinearHash(idxname, )
   		}
   		else {
   			hcatInfo = 
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