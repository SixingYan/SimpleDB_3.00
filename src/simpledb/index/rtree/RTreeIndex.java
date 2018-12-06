package simpledb.index.rtree;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.file.Block;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.FloatConstant;
import simpledb.query.IntConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;

public class RTreeIndex implements Index {

	private RTreeControl RTCL;
	private String idxname;
	private Schema rsch;
	private Transaction tx;
	private TableInfo rlp, rp;
	static final float Ruler = 1;
	private int root;
	static final float UserX = 2;
	static final float UserY = 2;

	public RTreeIndex(String idxname, Schema rsch, Transaction tx) {
		HashMap<Integer, RPage>treelist = new HashMap<Integer, RPage>();
		root = 0;
		this.idxname = idxname;
		this.rsch = rsch;
		this.tx = tx;
		Schema rlpsch = new Schema();
		rlpsch.addIntField("pid");
		rlpsch.addIntField("pp");
		rlpsch.addIntField("Px");
		rlpsch.addIntField("Py");
		rlpsch.addIntField("Pb");
		rlpsch.addIntField("Pd");
		rlp = new TableInfo("rlp", rlpsch);
		if (tx.size(rlp.fileName()) == 0) {
			tx.append(rlp.fileName(), new RTPageFormatter(rlp, 0));
			System.out.println("Append!");
		} else {
			int size = tx.size(rlp.fileName());
			System.out.println("size: " + size);
			for (int i = 0; i < size; i++) {
				HashMap<String, Integer> intValue = new HashMap<String, Integer>();
				Block leaf = new Block(rlp.fileName(), i);
				tx.pin(leaf);
				for (String fldname : rlp.schema().fields()) {
					int offset = rlp.offset(fldname);
					//System.out.println(fldname);
					int value = tx.getInt(leaf, offset);
					intValue.put(fldname, value);
				}
				tx.unpin(leaf);
				Point P = new Point(intValue.get("Px"), intValue.get("Py"));
				RID rid = new RID(intValue.get("Pb"), intValue.get("Pd"));
				P.setData(rid);
				System.out.println("RID: " + P.getData());
				if (!(treelist.containsKey(intValue.get("pid")))) {
					System.out.println("New!");
					RLeafPage RLP = new RLeafPage(P, Ruler, Ruler);
					RLP.id = intValue.get("pid");
					RLP.parent = intValue.get("pp");
					treelist.put(RLP.id, RLP);
					//treeliststore.put(RLP.id,RLP);
				} else {
					System.out.println("Not New!");
					((RLeafPage)treelist.get(intValue.get("pid"))).merge(P);
					//((RLeafPage)treeliststore.get(intValue.get("pid"))).merge(P);
				}
			}

		}

		Schema rpsch = new Schema();
		rpsch.addIntField("id");
		rpsch.addIntField("parent");
		rpsch.addIntField("xstart");
		rpsch.addIntField("xend");
		rpsch.addIntField("ystart");
		rpsch.addIntField("yend");
		rpsch.addIntField("root");
		rpsch.addStringField("child", 10);
		rp = new TableInfo("rp", rpsch);
		if (tx.size(rp.fileName()) == 0) {
			tx.append(rp.fileName(), new RTPageFormatter(rp, 0));
			System.out.println("Append!");
		} else {
			int size = tx.size(rp.fileName());
			for (int i = 0; i < size; i++) {
				HashMap<String, Integer> intValue = new HashMap<String, Integer>();
				String childlist = null;
				Block rpage = new Block(rp.fileName(), i);
				tx.pin(rpage);
				for (String fldname : rp.schema().fields()) {
					int offset = rp.offset(fldname);
					if (rp.schema().type(fldname) == INTEGER) {
						//System.out.println(fldname);
						int value = tx.getInt(rpage, offset);
						intValue.put(fldname, value);
					} else {
						//System.out.println(fldname);
						childlist = tx.getString(rpage, offset);
					}
				}
				tx.unpin(rpage);
				Rectangle R = new Rectangle(intValue.get("xstart"), intValue.get("xend"), intValue.get("ystart"), intValue.get("yend"));
				RPage RP = new RPage(R);
				RP.id = intValue.get("id");
				RP.parent = intValue.get("parent");
				System.out.println(childlist);
				String[] childs = childlist.split("\\|");
				for (int j = 0; j < childs.length; j++) {
					RP.childs.add(Integer.parseInt(childs[j]));
				}
				if (intValue.get("root") == 1) {
					root = RP.id;
				}
				treelist.put(RP.id, RP);
				//treeliststore.put(RP.id,RP);
			}
		}

		RTCL = new RTreeControl(treelist, treelist.size(), root);
		//System.out.println("Start with:");
		//RTCL.PrintList();
	}


	@Override
	public void beforeFirst(Constant searchkey) {
		// TODO Auto-generated method stub
		int key = (Integer) ((IntConstant)searchkey).asJavaVal();

		Point P = new Point(UserX, UserY);
		RTCL.beforeFirst(P, key);
	}

	@Override
	public boolean next() {
		// TODO Auto-generated method stub
		return RTCL.next();
	}

	@Override
	public RID getDataRid() {
		// TODO Auto-generated method stub
		System.out.println("RID:" + RTCL.getDataRid());
		return RTCL.getDataRid();
	}

	//@Override
	public void insert(ArrayList<Constant> vals, RID datarid) {
		// TODO Auto-generated method stub
		float x = (Float) ((FloatConstant)vals.get(0)).asJavaVal();
		float y = (Float) ((FloatConstant)vals.get(1)).asJavaVal();
		Point P = new Point(x, y);
		P.setData(datarid);
		RTCL.insert(P);

	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		int newroot = RTCL.getRoot();
		int countrp = 0;
		int countrlp = 0;
		// TODO Auto-generated method stub
		HashMap<Integer, RPage> treelist = RTCL.getList();
		RTCL.PrintList();
		ArrayList<Point> pointList = new ArrayList<Point>();


		for (int i = 0; i < treelist.size(); i++) {
			if (treelist.get(i).getClass() == RLeafPage.class) {
				RLeafPage NewRLP = (RLeafPage)treelist.get(i);
				for (int j = 0; j < NewRLP.points.size(); j++) {
					Point P = NewRLP.points.get(j);
					P.id = NewRLP.id;
					P.parent = NewRLP.parent;
					pointList.add(P);
				}
			} else {
				RPage RP = treelist.get(i);
				Block rpage = new Block(rp.fileName(), countrp);
				System.out.println(countrp);
				countrp++;
				tx.pin(rpage);
				tx.setInt(rpage, rp.offset("id"), RP.id);
				tx.setInt(rpage, rp.offset("parent"), RP.parent);
				tx.setInt(rpage, rp.offset("xstart"), (int)RP.getRegion().getXstart());
				tx.setInt(rpage, rp.offset("ystart"), (int)RP.getRegion().getYstart());
				tx.setInt(rpage, rp.offset("xend"), (int)RP.getRegion().getXend());
				tx.setInt(rpage, rp.offset("yend"), (int)RP.getRegion().getYend());
				System.out.println("Parent: " + tx.getInt(rpage, rp.offset("parent")));
				System.out.println("ID: " + tx.getInt(rpage, rp.offset("id")));
				writechild(RP.childs, rpage);

				int isroot = tx.getInt(rpage, rp.offset("root"));

				if (newroot == RP.id) {
					if (isroot == 0) {
						tx.setInt(rpage, rp.offset("root"), 1);
					}
				} else {
					if (isroot == 1) {
						tx.setInt(rpage, rp.offset("root"), 0);
					}
				}

				tx.unpin(rpage);
			}
		}
		System.out.println("Points:" + pointList.size());
		for (int i = 0; i < pointList.size(); i++) {
			Point P = pointList.get(i);
			Block blk = new Block(rlp.fileName(), i);
			tx.pin(blk);
			tx.setInt(blk, rlp.offset("pid"), P.id);
			tx.setInt(blk, rlp.offset("pp"), P.parent);
			tx.setInt(blk, rlp.offset("Px"), (int)P.getX());
			tx.setInt(blk, rlp.offset("Py"), (int)P.getY());
			tx.setInt(blk, rlp.offset("Pb"), P.getData().blockNumber());
			tx.setInt(blk, rlp.offset("Pd"), P.getData().id());
			tx.unpin(blk);
		}
		tx.commit();

	}


	private void writechild(ArrayList<Integer> childs, Block blk) {
		String cid = null;
		for (int i = 0; i < childs.size(); i++) {
			if (i == 0)
				cid = String.valueOf(childs.get(i));
			else
				cid = cid + "|" + childs.get(i);

		}
		tx.setString(blk, rp.offset("child"), cid);
	}

	@Override
	public void insert(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub
	}
}
