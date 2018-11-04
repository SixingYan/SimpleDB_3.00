package simpledb.metadata;

import static simpledb.metadata.TableMgr.MAX_NAME;
import simpledb.tx.Transaction;
import simpledb.record.*;
import java.util.*;

/**
 * The index manager.
 * The index manager has similar functionalty to the table manager.
 * @author Edward Sciore
 */
public class IndexMgr {
   private TableInfo ti;
   private LinearHashIndexMgr lhiMgr;

   /**
	* Creates the index manager.
	* This constructor is called during system startup.
	* If the database is new, then the <i>idxcat</i> table is created.
	* @param isnew indicates whether this is a new database
	* @param tx the system startup transaction
	*/
	public IndexMgr(boolean isnew, TableMgr tblmgr, Transaction tx) {
	  	if (isnew) {
	  		initIndex(tblmgr, tx);
	  		initIndexFileMgr(tblmgr, tx);
	  	}
	  this.ti = tblmgr.getTableInfo("idxcat", tx);
	  this.lhiMgr = new LinearHashIndexMgr();
   	}

	private void initIndex (TableMgr tblmgr, Transaction tx) {
		Schema sch = new Schema();
		sch.addStringField("indexname", MAX_NAME);
		sch.addStringField("tablename", MAX_NAME);
		sch.addStringField("fieldname", MAX_NAME);
		tblmgr.createTable("idxcat", sch, tx);
   	}

   	private void initIndexFileMgr (TableMgr tblmgr, Transaction tx) {
   		Schema sch = new Schema();
		sch.addStringField("indexname", MAX_NAME);
		sch.addIntField("split");
		sch.addIntField("round");
		tblmgr.createTable("lhidxcat", sch, tx);
   	}

   /**
	* Creates an index of the specified type for the specified field.
	* A unique ID is assigned to this index, and its information
	* is stored in the idxcat table.
	* @param idxname the name of the index
	* @param tblname the name of the indexed table
	* @param fldname the name of the indexed field
	* @param tx the calling transaction
	*/
   public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
	  	RecordFile rf = new RecordFile(ti, tx);
	  	rf.insert();
	  	rf.setString("indexname", idxname);
	  	rf.setString("tablename", tblname);
	  	rf.setString("fieldname", fldname);
	  	rf.close();

	  	if (idxtype.equals("linearhash") & this.lhti == Null)
	  		createLinearHashIndex(tblname, idxname, tx);
	
	}

	/**
	* say some things.
	* @param tx the calling transaction
	*/
   	public void createLinearHashIndex(Transaction tx) {
		Schema lhsch = new Schema();
		lhsch.addStringField("indexname", MAX_NAME);
		lhsch.addIntField("split");
		lhsch.addIntField("round");
		SimpleDB.mdmgr().tblmgr.createTable("lhashcat", lhsch, tx);

		Schema lhfsch = new Schema();
		lhfsch.addIntField("blocknumber");
		String lhfno;
		for (int i = 0; i < INIT_HASH_TBL_SIZE; i ++) {
			lhfno = (tblname + idxname).hashCode() + "_" + i;
			SimpleDB.mdmgr().tblmgr.createTable("lhashcat" + lhfno, lhfsch, tx);
		}
   	}


   /**
	* Returns a map containing the index info for all indexes
	* on the specified table.
	* @param tblname the name of the table
	* @param tx the calling transaction
	* @return a map of IndexInfo objects, keyed by their field names
	*/
   public Map<String,IndexInfo> getIndexInfo(String tblname, Transaction tx) {
	  Map<String,IndexInfo> result = new HashMap<String,IndexInfo>();
	  RecordFile rf = new RecordFile(ti, tx);
	  while (rf.next())
		 if (rf.getString("tablename").equals(tblname)) {
		 String idxname = rf.getString("indexname");
		 String fldname = rf.getString("fieldname");
		 IndexInfo ii = new IndexInfo(idxname, tblname, fldname, tx);
		 result.put(fldname, ii);
	  }
	  rf.close();
	  return result;
   }
}
