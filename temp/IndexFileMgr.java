public class IndexFileMgr {
	private String idxFilename;
	private Map<String, Map> idxFile;

	public IndexFileMgr (String filename) {
		this.idxFilename = filename; //?
	}

	public Map<String, Map<Integer,List>> get() {
		read();
		return 
	}

	public void put(Map<String, Map> config, Map index) {
		Map indexFile = new Map<String, Map>();
		indexFile.put("config", config);
		indexFile.put("index", index);
		save();
	}
	private void save? () {
		// 可能会换一个名称
		this.idxFile;
	}
}