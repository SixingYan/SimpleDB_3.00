public class LHashBucket {
	private Block currentblk;
	private TableInfo ti;
	private Transaction tx;
	private int size=0;
	// slot 位置用 hashCode() % NUM_SIZE 来直接找到

	LHashBucket(Block currentblk, TableInfo ti, Transaction tx) {
		this.currentblk = currentblk;
      	this.ti = ti;
      	this.tx = tx;
      	slotsize = ti.recordLength();
      	this.slotnum = (BLOCK_SIZE - INT_SIZE - INT_SIZE) / this.slotsize - 1 // 保留出一个
      	tx.pin(currentblk);

	}
	/**
    * Calculates the position where the first record having
    * the specified search key should be, then returns
    * the position before it.
    * @param searchkey the search key
    * @return the position before where the search key goes
    */
	public findSlotBefore(Constant searchkey) {
		int slot = searchkey.hashCode() % this.slotnum;
		return slot - 1;
	}
	
	/**
    * Closes the page by unpinning its buffer.
    */
   	public void close() {
      	if (this.currentblk != null)
         	this.tx.unpin(currentblk);
      	this.currentblk = null;
   	}



	public  insert() {

	}

	public Boolean isFull() {
		// 通过block的位移判断是否已经到
		return slotpos(getNumRecs()+1) >= BLOCK_SIZE;
	}
	
	// called by LHashFunction
	/**
    * Splits the page at the specified position.
    * A new page is created, and the records of the page
    * starting at the split position are transferred to the new page.
    * @param splitpos the split position
    * @return the reference to the new block
    */
	public void split() {

	}
	
	/**
    * Appends a new block to the end of the specified B-tree file,
    * having the specified flag value.
    * @return a reference to the newly-created block
    */
	public Block appendNew() {
      return tx.append(ti.fileName(), new LHPageFormatter(ti, flag));
   	}

	/**
	* Returns the number of index records in this page.
	* @return the number of index records in this page
	*/
	public int getNumRecs() {
		return this.tx.getInt(currentblk, INT_SIZE);
	}

	private int fldpos(int slot, String fldname) {
      	int offset = this.ti.offset(fldname);
      	return slotpos(slot) + offset;
   	}

   	private int slotpos(int slot) {
      	return INT_SIZE + INT_SIZE + (slot * slotsize);
   	}

}