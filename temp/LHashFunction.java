public class LHashFunction {
	private int count;
	private int ;
	// 是否还需要每个bucket的数量
	
	LHashFunction() {

	}

	public int search(Constant searchkey) {
		int key = searchkey.hashCode();
		int bktnum = hash(key, this.round);
		if (bktnum < this.split) 
			bktnum = hash(key, this.round + 1);
		return bktnum;
	}

	insert() {

	}

	delete() {

	}
	
	// private method
	private int hash(int key, int round) {
		return key % (this.count * round);
	}
}