package simpledb.index.rtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RPage {
	private Rectangle Region;
	private int level;
	public ArrayList<Integer> childs=new ArrayList<Integer>();
	public int parent;
	public int id;
	
	public RPage(Rectangle R) {
		Region=R;
	}
	
	public RPage() {
		
	}
	
	public void Refresh(Rectangle R) {
		Region=R;
	}
	
	public void merge(Rectangle R) {
		Region.MergeR(R);
	}
	
	
	public void setLevel(int level) {
		this.level=level;
	}
	
	public int getLevel() {
		return level;
	}
	
	public boolean contains(Rectangle R) {
		if(Region.containR(R)) {
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public boolean overlaps(Rectangle R) {
		if(Region.overlap(R)) {
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public Rectangle getRegion() {
		return Region;
	}
	
}
