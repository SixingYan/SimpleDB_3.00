package simpledb.index.rtree;

public class Point {
	private float x;
	private float y;
	private String name;
	
	
	public Point(float x, float y, String name) {
		this.x=x;
		this.y=y;
		this.name=name;
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
		if(x==P.getX()&&y==P.getY()) {
			return true;
		}
		else
		{
			return false;
		}
	}
}
