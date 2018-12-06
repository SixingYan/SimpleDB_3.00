import static simpledb.metadata.TableMgr.MAX_NAME;

import java.util.ArrayList;
import java.util.Iterator;

import simpledb.index.rtree.RTreeIndex;
import simpledb.parse.InsertData;
import simpledb.parse.Parser;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author Xinmeng Gu | Sixing Yan
 */
public class CreateRTreeIndex {
	private String idxname;
	private ArrayList<String> xyflds = new ArrayList<String>();

	public CreateRTreeIndex() {}
	
	/**
	 * rtree index file
	 * @param tblname
	 * @param xname
	 * @param yname
	 */
	public void BuildRTree(String tblname, String xname, String yname) {
		this.idxname = tblname + "ridxcat";
		Transaction tx = new Transaction();
		boolean isNew = SimpleDB.fileMgr().isNew();
		if (isNew) {
			Schema sch = new Schema();
			sch.addStringField("indexname", MAX_NAME);
			sch.addStringField("tablename", MAX_NAME);
			sch.addStringField("xname", MAX_NAME);
			sch.addStringField("yname", MAX_NAME);
			SimpleDB.mdMgr().createTable(idxname, sch, tx);

		}
		TableInfo ti = SimpleDB.mdMgr().getTableInfo(idxname, tx);

		RecordFile rf = new RecordFile(ti, tx);
		rf.insert();
		rf.setString("indexname", idxname);
		rf.setString("tablename", tblname);
		rf.setString("xname", xname);
		rf.setString("yname", yname);
		rf.close();
		xyflds.add(xname);
		xyflds.add(yname);
		tx.commit();
	}

	public void insert(String cmd) {
		Transaction tx = new Transaction();
		Parser ps = new Parser(cmd);
		InsertData data = ps.insert();

		ArrayList<Constant> xyvalue = new ArrayList<Constant>();
		Plan p = new TablePlan(data.tableName(), tx);
		TableScan Us = (TableScan) p.open();
		Us.insert();
		RID rid = Us.getRid();
		Iterator<Constant> valIter = data.vals().iterator();
		Constant xvalue = null;
		Constant yvalue = null;

		for (String fldname : data.fields()) {
			Constant val = valIter.next();
			System.out.println("Modify field " + fldname + " to val " + val);
			Us.setVal(fldname, val);
			if (fldname.equals(xyflds.get(0)))
				xvalue = val;
			if (fldname.equals(xyflds.get(1)))
				yvalue = val;
		}
		
		Us.close();
		tx.commit();
		xyvalue.add(xvalue);
		xyvalue.add(yvalue);
		RTreeIndex rtid = new RTreeIndex(idxname, new Schema(), tx);

		rtid.insert(xyvalue, rid);
		rtid.close();
	}
}
