package simpledb.index.rtree;

import simpledb.record.RID;

public class Point {
	private float x;
	private float y;
	private String name;
	private RID rid;
	public int id;
	public int parent;

	public Point(float x, float y, String name) {
		this.x = x;
		this.y = y;
		this.name = name;
	}

	public Point(float x, float y) {
		this.x = x;
		this.y = y;
	}

	public Point(float x, float y, String name, RID rid) {
		this.x = x;
		this.y = y;
		this.name = name;
		this.rid = rid;
	}

	public float getX() {
		return x;
	}

	public float getY() {
		return y;
	}


	public String getName() {
		return name;
	}


	public boolean Same(Point P) {
		if (x == P.getX() && y == P.getY()) {
			return true;
		} else {
			return false;
		}
	}

	public void setData(RID rid) {
		this.rid = rid;
	}

	public RID getData() {
		return rid;
	}

	public boolean SameData(Point P) {
		if ((x == P.getX()) && (y == P.getY()) && (rid.blockNumber() == P.getData().blockNumber()) && (rid.id() == P.getData().id())) {
			return true;
		} else {
			return false;
		}
	}
}
