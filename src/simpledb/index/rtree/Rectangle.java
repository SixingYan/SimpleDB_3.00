package simpledb.index.rtree;

public class Rectangle {
	private float xstart;
	private float xend;
	private float ystart;
	private float yend;

	public Rectangle(Point p1, float xextend, float yextend) {
		this.xstart = p1.getX() - xextend;
		this.xend = p1.getX() + xextend;
		this.ystart = p1.getY() - yextend;
		this.yend = p1.getY() + yextend;
	}

	public Rectangle() {

	}

	public Rectangle(Rectangle R) {
		this.xstart = R.getXstart();
		this.xend = R.getXend();
		this.ystart = R.getYstart();
		this.yend = R.getYend();
	}

	public Rectangle(Point p1, Point p2) {
		if (p1.getX() < p2.getX()) {
			this.xstart = p1.getX();
			this.xend = p2.getX();
		} else {
			this.xstart = p2.getX();
			this.xend = p1.getX();
		}
		if (p1.getY() < p2.getY()) {
			this.ystart = p1.getY();
			this.yend = p2.getY();
		} else {
			this.ystart = p2.getY();
			this.yend = p1.getY();
		}
	}

	public Rectangle(float xs, float xe, float ys, float ye) {
		this.xstart = xs;
		this.xend = xe;
		this.ystart = ys;
		this.yend = ye;
	}

	public void Merge(Point p) {
		if (!contain(p.getX(), p.getY())) {
			if (p.getX() < xstart)
				xstart = p.getX();
			if (p.getX() > xend)
				xend = p.getX();
			if (p.getY() < ystart)
				ystart = p.getY();
			if (p.getY() > yend)
				yend = p.getY();
		}

	}

	public void MergeR(Rectangle R) {
		if (!containR(R)) {
			if (R.getXstart() < xstart) {
				xstart = R.getXstart();
			}
			if (R.getXend() > xend) {
				xend = R.getXend();
			}
			if (R.getYstart() < ystart) {
				ystart = R.getYstart();
			}
			if (R.getYend() > yend) {
				yend = R.getYend();
			}
		}


	}

	public boolean overlap(Rectangle R) {
		if (contain(R.getXstart(), R.getYstart())) {
			return true;
		} else if (contain(R.getXend(), R.getYstart())) {
			return true;
		} else if (contain(R.getXstart(), R.getYend())) {
			return true;
		} else if (contain(R.getXend(), R.getYend())) {
			return true;
		} else {
			if (R.contain(getXstart(), getYstart())) {
				return true;
			} else if (R.contain(getXend(), getYstart())) {
				return true;
			} else if (R.contain(getXstart(), getYend())) {
				return true;
			} else if (R.contain(getXend(), getYend())) {
				return true;
			} else {
				return false;
			}

		}
	}

	public boolean containR(Rectangle R) {
		if (contain(R.getXstart(), R.getYstart())) {
			if (contain(R.getXend(), R.getYstart())) {
				if (contain(R.getXstart(), R.getYend())) {
					if (contain(R.getXend(), R.getYend())) {
						return true;
					}
				}
			}
		}

		return false;
	}

	public boolean contain(float x, float y) {
		if (x > xstart && x < xend && y > ystart && y < yend) {
			return true;
		} else {
			return false;
		}
	}

	public float cost(Point P) {
		float cost;
		if (contain(P.getX(), P.getY())) {
			return 0;
		} else {
			Rectangle R1 = new Rectangle(xstart, xend, ystart, yend);
			R1.Merge(P);
			return Math.abs(R1.area() - this.area());
		}
	}

	public float area() {
		return Math.abs((xend - xstart) * (yend - ystart));
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

	public float intersect(Rectangle R) {
		if (!overlap(R)) {
			return 0;
		} else {
			int[] count = new int[4];
			if (contain(R.getXstart(), R.getYstart())) {
				count[0] = 1;
			}
			if (contain(R.getXstart(), R.getYend())) {
				count[1] = 1;
			}
			if (contain(R.getXend(), R.getYstart())) {
				count[2] = 1;
			}
			if (contain(R.getXend(), R.getYend())) {
				count[3] = 1;
			}
			int counttime = 0;
			for (int i = 0; i < 4; i++) {
				if (count[i] == 1) {
					counttime++;
				}
			}

			if (counttime == 1) {
				if (count[0] == 1) {
					return ((xend - R.getXstart()) * (yend - R.getYstart()));
				}
				if (count[1] == 1) {
					return ((xend - R.getXstart()) * (R.getYend() - ystart));
				}
				if (count[2] == 1) {
					return ((R.getXend() - xstart) * (yend - R.getYstart()));
				}
				if (count[3] == 1) {
					return ((R.getXend() - xstart) * (R.getYend() - ystart));
				}
			} else if (counttime == 2) {
				if (count[0] == 1 && count[1] == 1) {
					return ((xend - R.getXstart()) * (R.yend - R.ystart));
				}
				if (count[1] == 1 && count[3] == 1) {
					return ((R.getYend() - ystart) * (R.xend - R.xstart));
				}
				if (count[3] == 1 && count[2] == 1) {
					return ((R.getXend() - xstart) * (R.yend - R.ystart));
				}
				if (count[2] == 1 && count[0] == 1) {
					return ((yend - R.getYstart()) * (R.xend - R.xstart));
				}
			} else if (counttime == 4) {
				return R.area();
			} else {
				return -1;
			}
			return -1;
		}
	}
}
