package simpledb.index.rtree;

public class Rectangle {
	private float xstart;
	private float xend;
	private float ystart;
	private float yend;
	
	public Rectangle(Point p1, float xextend, float yextend) {
		this.xstart=p1.getX()-xextend;
		this.xend=p1.getX()+xextend;
		this.ystart=p1.getY()-yextend;
		this.yend=p1.getY()+yextend;  
	}
	
	public Rectangle(Point p1, Point p2) {
		if(p1.getX()<p2.getX()) {
			this.xstart=p1.getX();
			this.xend=p2.getX();
		}
		else
		{
			this.xstart=p2.getX();
			this.xend=p1.getX();
		}
		if(p1.getY()<p2.getY()) {
			this.ystart=p1.getY();
			this.yend=p2.getY();
		}
		else
		{
			this.ystart=p2.getY();
			this.yend=p1.getY();
		}
	}
	
	public void Merge(Point p) {
		if(p.getX()<xstart)
			xstart=p.getX();
		if(p.getX()>xend)
			xend=p.getX();
		if(p.getY()<ystart)
			ystart=p.getY();
		if(p.getY()<yend)
			yend=p.getY();
	}
	
	public void MergeR(Rectangle R) {
		if(R.getXstart()<xstart) {
			xstart=R.getXstart();
		}
		if(R.getXend()>xend) {
			xend=R.getXend();
		}
		if(R.getYstart()<ystart) {
			ystart=R.getYstart();
		}
		if(R.getYend()<yend) {
			yend=R.getYend();
		}
		
	}
	
	public boolean overlap(Rectangle R) {
		if(contain(R.getXstart(), R.getYstart())) {
			return true;
		}
		else if(contain(R.getXend(), R.getYstart())) {
			return true;
		}
		else if(contain(R.getXstart(), R.getYend())) {
			return true;
		}
		else if(contain(R.getXend(), R.getYend())) {
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public boolean containR(Rectangle R) {
		if(contain(R.getXstart(), R.getYstart())) {
			if(contain(R.getXend(), R.getYstart())) {
				if(contain(R.getXstart(), R.getYend())) {
					if(contain(R.getXend(), R.getYend())) {
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	public boolean contain(float x, float y) {
		if(x>xstart&&x<xend&&y>ystart&&y<yend) {
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public float cost(Point P) {
		float cost;
		if(contain(P.getX(),P.getY())) {
			return 0;
		}
		else
		{
			Rectangle R1 = this;
			R1.Merge(P);
			return R1.area()-this.area();
		}
	}
	
	public float area() {
		return (xend-xstart)*(yend-ystart);
	}
	
	public float getXstart() {
		return xstart;
	}
	
	public float getXend() {
		return xend;
	}
	
	public float getYstart() {
		return ystart;
	}
	
	public float getYend() {
		return yend;
	}
	

}
