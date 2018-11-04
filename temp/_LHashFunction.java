BUCKET_SIZE // 桶容量

public class LHashFunction {
	
	private int round; // 第几回合
	private int split; // 分裂点坐标
	private int size; // function的最大bucket数量
	private int count; // 目前有多少个bucket

	// page format |round|split|size|count|blk

	// 需要存储 blknum，因为这个不一定是自增的
	LHashFunction() {
		
	}

	public int search(Constant searchkey) {
		int key = searchkey.hashCode();
		int bktnum = hash(key, this.round);
		if (bktnum < this.split) 
			bktnum = hash(key, this.round + 1);
		return bktnum;
	}

	
	delete() {

	}
	
	split(LHashBucket blk) {
		blk.split();
		this.count ++;
		this.split ++;

		if (this.split >= this.count) {
			this.round ++;
			this.size = this.size * 2;
			this.split = 0;
		}
	}

	// private method
	private int hash(int key, int round) {
		return key % (this.count * round);
	}


	private int getCount() {

	}

	private void setCount() {

	}

	private void 
	private void setSize() {

	}


}