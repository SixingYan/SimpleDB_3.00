package simpledb.index.rtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;



public class RLeafPage extends RPage {
	public Rectangle Region;
	public ArrayList<Point> points = new ArrayList<Point>();
	int one;


	
	public RLeafPage(Point p1,Point p2) {
		super();
		points.add(p1);
		points.add(p2);
		Region=new Rectangle(p1,p2);
		one=0;
	}
	
	public RLeafPage(Point p, float xextend, float yextend) {
		super();
		Region=new Rectangle(p,xextend,yextend);
		points.add(p);
		one=1;
	}
	
	public void merge(Point p) {
		if(one==1)
		{
			Region=new Rectangle(points.get(0),p);
			points.add(p);
			one=0;
		}
		else
		{
			points.add(p);
			Region.Merge(p);
		}
	}
	
	
	public void refresh() {
		if(points.size()==2) {
			Region=new Rectangle(points.get(0),points.get(1));
		}
		else
		{
			Region=new Rectangle(points.get(0),points.get(1));
			for(int i=2;i<points.size();i++) {
				Region.Merge(points.get(i));
			}
		}
		
	}
	
	
}
