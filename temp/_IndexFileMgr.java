// 不能用二进制做

public class IndexFileMgr {
	private String idxFilename;
	private Map<String, Map> idxFile;
	// 这里全局地址怎么拿到？
	//System.getProperty("user.home");
	// add a method to get dbDirectory to 
	SimpleDB.fileMgr().dbDir();
	public IndexFileMgr (String filename) {
		this.idxFilename = filename; //?
	}

	public Boolean hasIndexFile () {
		// if not, initialization
		this.indexFile = new Map<String, Map>();
	} 

	public Map<String, Map<Integer,List>> get() {
		read();
		return this.idxFile; 
	}

	public void put(Map<String, Map> config, Map index) {
		this.indexFile.put("config", config);
		this.indexFile.put("index", index);
		write();
	}
	private void write (){
		this.idxFile;
	}

	private Map read () {
		this.idxFile
	}


	private void createIndexFile () {
		lhashcat

		lhashfcat +

	}
}