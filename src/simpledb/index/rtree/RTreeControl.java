package simpledb.index.rtree;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.record.RID;



public class RTreeControl {
	private HashMap<Integer, RPage> treelist = new HashMap<Integer, RPage>();
	private int root = 0;
	private int isnull = 1;
	static final float Ruler = 1;
	static final float SearchRuler = 3;
	static final int Limit1 = 3;
	static final int Limit2 = 3;
	static final int DownLimit1 = 2;
	static final int DownLimit2 = 2;
	private ArrayList<Point> resultList = new ArrayList<Point>();
	private ArrayList<RPage> deleteList = new ArrayList<RPage>();
	private ArrayList<Point> PointWaitingList = new ArrayList<Point>();
	private ArrayList<RPage> PageWaitingList = new ArrayList<RPage>();
	private int depth = 0;
	private int Chose;
	private int idGiver = 0;
	private int checkdepth;
	private int pointer;

	public RTreeControl() {

	}


	public RTreeControl(HashMap<Integer, RPage> treelist, int idGiver, int root) {
		this.treelist = treelist;
		this.idGiver = idGiver;
		this.root = root;
		if (treelist.size() == 0) {
			isnull = 1;
			this.idGiver = 0;
		} else {
			isnull = 0;
		}
	}


	public RPage NewRPage() {
		RPage NewPage = new RPage();
		NewPage.id = idGiver;
		idGiver++;
		return NewPage;
	}

	public RPage NewRPageR(Rectangle R) {
		RPage NewPage = new RPage(R);
		NewPage.id = idGiver;
		idGiver++;
		return NewPage;
	}



	public ArrayList<Point> search(Point UserP, int key) {
		resultList = new ArrayList<Point>();
		Rectangle searchR = new Rectangle(UserP, key, key);
		//System.out.println("x start: "+searchR.getXstart());
		//System.out.println("x end: "+searchR.getXend());
		//System.out.println("y start: "+searchR.getYstart());
		//System.out.println("y end: "+searchR.getYend());
		searchlevel(treelist.get(root), searchR);
		return resultList;
	}

	public void searchlevel(RPage RP, Rectangle searchR) {
		for (int i = 0; i < RP.childs.size(); i++) {
			int child = RP.childs.get(i);
			if (treelist.get(child).getClass() == RLeafPage.class) {
				RLeafPage RLP = (RLeafPage)treelist.get(child);
				//System.out.println("Leaf:"+RLP.id);
				if (RLP.overlaps(searchR)) {
					for (int j = 0; j < RLP.points.size(); j++) {

						if (searchR.contain(RLP.points.get(j).getX(), RLP.points.get(j).getY())) {
							resultList.add(RLP.points.get(j));
						}
					}
				}
			} else {
				RPage ChildRP = (RPage)treelist.get(child);
				//System.out.println("RP: "+ChildRP.id);
				if (ChildRP.overlaps(searchR)) {
					//System.out.println("overlap!");
					searchlevel(ChildRP, searchR);
				}
			}
		}

	}

	public void insertPage(RPage RP) {
		checkdepth = 1;
		findPlace(treelist.get(root), RP);
		RPage TargetPage = treelist.get(Chose);
		RPage ParentPage = treelist.get(TargetPage.parent);
		if (ParentPage.childs.size() > Limit1) {
			RPage NewPage = new RPage();
			NewPage.Refresh(TargetPage.getRegion());
			NewPage.id = idGiver;
			idGiver++;
			NewPage.parent = ParentPage.parent;
			treelist.put(NewPage.id, NewPage);
			Adjusting(treelist.get(NewPage.parent), 1);
		} else {
			treelist.get(ParentPage.id).merge(TargetPage.getRegion());
			Adjusting(treelist.get(Chose), 0);
		}
	}

	public void findPlace(RPage RP, RPage searchR) {
		if (checkdepth == searchR.getLevel()) {
			if (RP.contains(searchR.getRegion())) {
				searchR.parent = RP.id;
				treelist.get(RP.id).childs.add(RP.id);
				treelist.put(searchR.id, searchR);
			}
		} else {
			for (int i = 0; i < RP.childs.size(); i++) {
				RPage ChildPage = treelist.get(RP.childs.get(i));
				if (ChildPage.contains(searchR.getRegion())) {
					checkdepth++;
					findPlace(ChildPage, searchR);
					break;
				}
			}
		}
	}

	public void insert(Point RestP) {
		if (isnull == 1) {

			RLeafPage NewLeaf = new RLeafPage(RestP, Ruler, Ruler);
			RPage Root = NewRPageR(NewLeaf.getRegion());
			treelist.put(Root.id, Root);
			NewLeaf.id = idGiver;
			idGiver++;
			NewLeaf.parent = root;
			treelist.put(NewLeaf.id, NewLeaf);
			treelist.get(root).childs.add(NewLeaf.id);
			depth++;
			isnull = 0;
			return;

		}
		chooseLeaf(treelist.get(root), RestP);
		RLeafPage TargetPage = (RLeafPage)treelist.get(Chose);
		if (TargetPage.points.size() >= Limit2) {
			RLeafPage NewPage = new RLeafPage(RestP, Ruler, Ruler);
			NewPage.id = idGiver;
			idGiver++;
			NewPage.parent = TargetPage.parent;
			treelist.put(NewPage.id, NewPage);
			//Divide2(TargetPage.id,NewPage.id,1);
			Divide(TargetPage.id, NewPage.id, 1);
			//PrintList();
			//System.out.println("chosed: "+TargetPage.id);
			//System.out.println("parent: "+NewPage.parent);
			Adjusting(NewPage, 1);
		} else {
			//System.out.println("chosed: "+Chose);
			((RLeafPage)treelist.get(Chose)).merge(RestP);
			Adjusting(treelist.get(Chose), 0);
		}


	}

	public void Adjusting(RPage NewRP, int flag) {

		if (flag == 0) {

			RefreshRP(NewRP.parent);
			if (NewRP.parent == root) {
				//System.out.println("root!");
				return;
			} else {
				//System.out.println("parent:"+NewRP.parent);
				Adjusting(treelist.get(NewRP.parent), 0);
			}
		} else {
			if (treelist.get(NewRP.parent).childs.size() == Limit1) {
				if (NewRP.parent == root) {
					//System.out.println("new root!");
					RPage OFRP = NewRPageR(NewRP.getRegion());
					OFRP.childs.add(NewRP.id);
					RPage NewRoot = NewRPageR(OFRP.getRegion());
					NewRoot.childs.add(OFRP.id);
					NewRoot.childs.add(NewRP.parent);
					OFRP.parent = NewRoot.id;
					treelist.get(NewRP.parent).parent = NewRoot.id;
					treelist.put(OFRP.id, OFRP);
					treelist.put(NewRoot.id, NewRoot);
					//System.out.println(OFRP.childs.get(0));
					//Divide2(NewRP.parent, OFRP.id, 0);
					Divide(NewRP.parent, OFRP.id, 0);

					RefreshRP(NewRoot.id);
					root = NewRoot.id;
					depth++;
					return;
				} else {
					//System.out.println("new!");
					RPage OFRP = NewRPageR(NewRP.getRegion());
					int targetid = NewRP.parent;
					OFRP.childs.add(NewRP.id);
					NewRP.parent = OFRP.id;
					OFRP.parent = treelist.get(targetid).parent;
					treelist.put(OFRP.id, OFRP);
					//Divide2(targetid,OFRP.id,0);
					Divide(targetid, OFRP.id, 0);

					Adjusting(OFRP, 1);
				}

			} else {
				if (NewRP.parent == root) {
					treelist.get(NewRP.parent).childs.add(NewRP.id);
					RefreshRP(NewRP.parent);
					return;
				} else {
					treelist.get(NewRP.parent).childs.add(NewRP.id);
					RefreshRP(NewRP.parent);
					Adjusting(treelist.get(NewRP.parent), 0);
				}

			}
		}

	}

	public void chooseLeaf(RPage RP, Point P) {
		float cost = 0;
		int flag = 0;
		int target = 0;
		for (int i = 0; i < RP.childs.size(); i++) {
			int child = RP.childs.get(i);
			if (treelist.get(child).getClass() == RLeafPage.class) {
				flag = 1;
				RLeafPage RLP = (RLeafPage)treelist.get(child);
				if (RLP.getRegion().contain(P.getX(), P.getY())) {
					Chose = child;
					return;
				} else {
					if (cost == 0) {
						cost = RLP.getRegion().cost(P);
						target = child;
					} else {
						if (cost > RLP.getRegion().cost(P)) {
							cost = RLP.getRegion().cost(P);
							target = child;

						}
					}
				}
			} else {
				RPage ChildRP = (RPage)treelist.get(child);
				if (ChildRP.getRegion().contain(P.getX(), P.getY())) {
					chooseLeaf(ChildRP, P);
				} else {
					if (cost == 0) {
						cost = ChildRP.getRegion().cost(P);
						target = child;
					} else {
						if (cost > ChildRP.getRegion().cost(P)) {
							cost = ChildRP.getRegion().cost(P);
							target = child;
						}
					}
				}
			}
		}

		if (flag == 0) {
			chooseLeaf(treelist.get(target), P);
		} else {
			Chose = target;
		}

	}


	public void delete(Point RestP) {
		resultList = new ArrayList<Point>();
		PointWaitingList = new ArrayList<Point>();
		deletesearching(treelist.get(root), RestP);

		for (int i = 0; i < deleteList.size(); i++) {
			checkdepth = depth;
			RLeafPage RLP = (RLeafPage)deleteList.get(i);
			if (RLP.points.size() == DownLimit2) {
				for (int j = 0; j < RLP.points.size(); j++) {

					if (!(RLP.points.get(j).Same(RestP))) {
						PointWaitingList.add(RLP.points.get(j));
					}
				}

				treelist.remove(RLP.id);
				RPage ParentR = treelist.get(RLP.parent);
				for (int j = 0; j < ParentR.childs.size(); j++) {
					if (ParentR.childs.get(j) == RLP.id) {
						treelist.get(RLP.parent).childs.remove(j);
						break;
					}
				}
				depth--;
				LevelDelete(RLP.parent, 0);

			} else {

				for (int j = 0; j < RLP.points.size(); j++) {
					if (RLP.points.get(j).Same(RestP)) {
						((RLeafPage)treelist.get(RLP.id)).points.remove(j);
						j--;
					}

				}

				((RLeafPage)treelist.get(RLP.id)).refresh();
				LevelDelete(RLP.parent, 0);

			}
		}

		for (int i = 0; i < PointWaitingList.size(); i++) {
			insert(PointWaitingList.get(i));
		}
		for (int i = 0; i < PageWaitingList.size(); i++) {
			insertPage(PageWaitingList.get(i));
		}
	}

	public void deletesearching(RPage RP, Point SearchP) {
		for (int i = 0; i < RP.childs.size(); i++) {
			int child = RP.childs.get(i);
			if (treelist.get(child).getClass() == RLeafPage.class) {
				RLeafPage RLP = (RLeafPage)treelist.get(child);
				if (RLP.getRegion().contain(SearchP.getX(), SearchP.getY())) {
					for (int j = 0; j < RLP.points.size(); j++) {
						if (RLP.points.get(j).Same(SearchP)) {
							deleteList.add(RLP);
						}

					}
				}
			} else {
				RPage ChildRP = (RPage)treelist.get(child);
				if (ChildRP.getRegion().contain(SearchP.getX(), SearchP.getY())) {
					deletesearching(ChildRP, SearchP);
				}
			}
		}
	}

	public void LevelDelete(int Deletedid, int flag) {
		if (flag == 0) {
			RPage DeletePage = treelist.get(Deletedid);
			treelist.get(Deletedid).Refresh(treelist.get(DeletePage.childs.get(0)).getRegion());
			for (int i = 1; i < DeletePage.childs.size(); i++) {
				treelist.get(Deletedid).getRegion().MergeR(treelist.get(DeletePage.childs.get(0)).getRegion());
			}
			if (Deletedid == root) {
				return;
			} else {
				LevelDelete(DeletePage.parent, 0);
			}
		} else {
			RPage DeletePage = treelist.get(Deletedid);
			if (DeletePage.childs.size() < DownLimit1) {

				if (DeletePage.id == root) {
					return;
				} else {
					DeletePage.setLevel(checkdepth);
					PageWaitingList.add(DeletePage);
					treelist.remove(DeletePage.id);
					RPage ParentR = treelist.get(DeletePage.parent);
					for (int j = 0; j < ParentR.childs.size(); j++) {
						if (ParentR.childs.get(j) == DeletePage.id) {
							treelist.get(DeletePage.parent).childs.remove(j);
							break;
						}
					}
					checkdepth--;
					LevelDelete(DeletePage.parent, 1);
				}


			} else {

				LevelDelete(Deletedid, 0);

			}
		}
	}

	public void PrintList() {
		System.out.println("-----------");
		System.out.println("root: " + root);
		for (int i = 0; i < treelist.size(); i++) {
			if (treelist.get(i).getClass() == RLeafPage.class) {
				RLeafPage RLP = (RLeafPage)treelist.get(i);
				System.out.println("RLP: " + RLP.id);
				Rectangle R = RLP.getRegion();
				System.out.println("x start: " + R.getXstart());
				System.out.println("x end: " + R.getXend());
				System.out.println("y start: " + R.getYstart());
				System.out.println("y end: " + R.getYend());
				for (int j = 0; j < RLP.points.size(); j++) {
					System.out.println(RLP.points.get(j).getData());
				}
			} else {
				RPage RP = treelist.get(i);
				System.out.println("RP: " + RP.id);
				Rectangle R = RP.getRegion();
				System.out.println("x start: " + R.getXstart());
				System.out.println("x end: " + R.getXend());
				System.out.println("y start: " + R.getYstart());
				System.out.println("y end: " + R.getYend());
				for (int j = 0; j < RP.childs.size(); j++) {
					System.out.println(RP.childs.get(j));
				}
			}
		}
		System.out.println("-----------");
	}

	/*
	public static void main(String args[]){
		RTreeControl RT=new RTreeControl();
		RT.insert(new Point(0,0,"a"));
		RT.PrintList();
		RT.insert(new Point(1,1,"b"));
		RT.PrintList();
		RT.insert(new Point(2,2,"c"));
		RT.PrintList();
		RT.insert(new Point(1,0,"d"));
		RT.PrintList();
		RT.insert(new Point(11,11,"e"));
		RT.PrintList();
		RT.insert(new Point(12,12,"f"));
		RT.PrintList();
		RT.insert(new Point(13,13,"g"));
		RT.PrintList();
		RT.insert(new Point(10,10,"h"));
		RT.PrintList();
		RT.insert(new Point(20,20,"i"));
		RT.PrintList();
		RT.insert(new Point(21,21,"j"));
		RT.PrintList();
		RT.insert(new Point(19,19,"k"));
		RT.PrintList();
		RT.insert(new Point(22,22,"l"));
		RT.PrintList();
		RT.insert(new Point(30,30,"m"));
		RT.PrintList();
		RT.insert(new Point(31,31,"n"));
		RT.PrintList();
		RT.insert(new Point(29,29,"o"));
		RT.PrintList();
		RT.insert(new Point(32,32,"p"));
		RT.PrintList();
		RT.insert(new Point(40,40,"q"));
		RT.PrintList();
		RT.insert(new Point(41,41,"r"));
		RT.PrintList();
		RT.insert(new Point(39,39,"s"));
		RT.PrintList();
		RT.insert(new Point(42,42,"t"));
		RT.PrintList();
		RT.insert(new Point(50,50,"u"));
		RT.PrintList();
		RT.insert(new Point(51,51,"v"));
		RT.PrintList();
		RT.insert(new Point(49,49,"w"));
		RT.PrintList();
		RT.insert(new Point(52,52,"x"));
		RT.PrintList();
		ArrayList<Point> result=RT.search(new Point(50,50,"user"));
		System.out.println("result size: "+result.size());
		for(int i=0;i<result.size();i++) {
			System.out.println(result.get(i).getName());
		}
	}*/


	public void Divide2(int page1, int page2, int flag) {
		if (flag == 0) {
			ArrayList<Integer> result1 = new ArrayList<Integer>();
			ArrayList<Integer> result2 = new ArrayList<Integer>();
			float inc = 0;
			RPage RP = treelist.get(page1);
			RPage RP2 = treelist.get(page2);
			ArrayList<RPage> Pages = new ArrayList<RPage>();
			for (int i = 0; i < RP.childs.size(); i++) {
				Pages.add(treelist.get(RP.childs.get(i)));
			}
			Pages.add(treelist.get(RP2.childs.get(0)));
			int seed1 = 0;
			int seed2 = 0;
			for (int i = 0; i < Pages.size() - 1; i++) {
				for (int j = i + 1; j < Pages.size(); j++) {
					Rectangle s1 = Pages.get(i).getRegion();
					Rectangle s2 = Pages.get(j).getRegion();
					float tempinc = increase(s1, s2);
					if (tempinc > inc) {
						inc = tempinc;
						seed1 = i;
						seed2 = j;
					}
				}
			}
			float inc1 = 0;
			float inc2 = 0;
			RPage seedPage1 = Pages.get(seed1);
			RPage seedPage2 = Pages.get(seed2);
			for (int i = 0; i < Pages.size(); i++) {
				if ((i != seed1) && (i != seed2)) {
					Rectangle R = Pages.get(i).getRegion();
					inc1 = increase(seedPage1.getRegion(), R);
					inc2 = increase(seedPage2.getRegion(), R);
					if (inc1 < inc2) {
						result1.add(Pages.get(i).id);
					} else {
						result2.add(Pages.get(i).id);
					}
				}
			}

			treelist.get(page1).childs = new ArrayList<Integer>();
			treelist.get(page1).childs.add(seedPage1.id);
			for (int i = 0; i < result1.size(); i++) {
				treelist.get(page1).childs.add(result1.get(i));
				treelist.get(result1.get(i)).parent = page1;
			}
			RefreshRP(page1);
			treelist.get(page2).childs = new ArrayList<Integer>();
			treelist.get(page2).childs.add(seedPage2.id);
			for (int i = 0; i < result2.size(); i++) {
				treelist.get(page2).childs.add(result2.get(i));
				treelist.get(result2.get(i)).parent = page2;
			}
			RefreshRP(page2);
		} else {
			ArrayList<Point> result1 = new ArrayList<Point>();
			ArrayList<Point> result2 = new ArrayList<Point>();
			float inc = 0;
			RLeafPage RLP = (RLeafPage)treelist.get(page1);
			RLeafPage RLP2 = (RLeafPage)treelist.get(page2);
			ArrayList<Point> Points = new ArrayList<Point>();
			for (int i = 0; i < RLP.points.size(); i++) {
				Points.add(RLP.points.get(i));
			}
			Points.add(RLP2.points.get(0));
			int seed1 = 0;
			int seed2 = 0;
			for (int i = 0; i < Points.size() - 1; i++) {
				for (int j = i + 1; j < Points.size(); j++) {
					Rectangle s1 = new Rectangle(Points.get(i), 0, 0);
					Rectangle s2 = new Rectangle(Points.get(j), 0, 0);
					float tempinc = increase(s1, s2);
					if (tempinc > inc) {
						inc = tempinc;
						seed1 = i;
						seed2 = j;
					}
				}
			}
			float inc1 = 0;
			float inc2 = 0;
			Point seedPoint1 = Points.get(seed1);
			Point seedPoint2 = Points.get(seed2);
			Rectangle seedRegion1 = new Rectangle(seedPoint1, 0, 0);
			Rectangle seedRegion2 = new Rectangle(seedPoint2, 0, 0);
			for (int i = 0; i < Points.size(); i++) {
				if ((i != seed1) && (i != seed2)) {
					Rectangle R = new Rectangle(Points.get(i), 0, 0);
					inc1 = increase(seedRegion1, R);
					inc2 = increase(seedRegion2, R);
					if (inc1 < inc2) {
						result1.add(Points.get(i));
					} else {
						result2.add(Points.get(i));
					}
				}
			}

			((RLeafPage)treelist.get(page1)).points = new ArrayList<Point>();
			((RLeafPage)treelist.get(page1)).points.add(seedPoint1);
			for (int i = 0; i < result1.size(); i++) {
				((RLeafPage)treelist.get(page1)).points.add(result1.get(i));
			}
			((RLeafPage)treelist.get(page1)).refresh();
			((RLeafPage)treelist.get(page2)).points = new ArrayList<Point>();
			((RLeafPage)treelist.get(page2)).points.add(seedPoint2);
			for (int i = 0; i < result2.size(); i++) {
				((RLeafPage)treelist.get(page2)).points.add(result2.get(i));
			}
			((RLeafPage)treelist.get(page2)).refresh();
		}
	}

	public void Divide(int page1, int page2, int flag) {
		int[] result = new int[4];
		int[] temp1;
		int[] temp2;
		float inc = 0;
		if (flag == 0) {
			Rectangle Catch1 = new Rectangle();
			Rectangle Catch2 = new Rectangle();
			RPage RP = treelist.get(page1);
			RPage RP2 = treelist.get(page2);
			ArrayList<RPage> Pages = new ArrayList<RPage>();
			for (int i = 0; i < RP.childs.size(); i++) {
				Pages.add(treelist.get(RP.childs.get(i)));
			}
			Pages.add(treelist.get(RP2.childs.get(0)));
			/*for(int a=0;a<Pages.size();a++) {
				System.out.println("Child:"+Pages.get(a).id);
			}*/
			for (int i = 0; i < Pages.size() - 1; i++) {
				for (int j = i + 1; j < Pages.size(); j++) {
					temp1 = new int[2];
					temp2 = new int[2];
					temp1[0] = i;
					temp1[1] = j;
					int m = 0;
					for (int k = 0; k < Pages.size(); k++) {
						if (k != i && k != j) {
							temp2[m] = k;
							m++;
							if (m == 2) {
								break;
							}
						}
					}
					Rectangle R1 = new Rectangle(Pages.get(temp1[0]).getRegion());
					R1.MergeR(Pages.get(temp1[1]).getRegion());
					//System.out.println(R1.area());
					Rectangle R2 = new Rectangle(Pages.get(temp2[0]).getRegion());
					R2.MergeR(Pages.get(temp2[1]).getRegion());
					//System.out.println(R2.area());
					float tempinc = increase(R1, R2);
					//System.out.println("tempinc: "+tempinc);
					if (tempinc > inc) {
						inc = tempinc;
						result[0] = temp1[0];
						result[1] = temp1[1];
						result[2] = temp2[0];
						result[3] = temp2[1];
						Catch1 = R1;
						Catch2 = R2;
					}
				}
			}
			//System.out.println(result[0]);
			//System.out.println(result[1]);
			//System.out.println(result[2]);
			//System.out.println(result[3]);
			treelist.get(page1).childs = new ArrayList<Integer>();
			treelist.get(page1).childs.add(Pages.get(result[0]).id);
			treelist.get(Pages.get(result[0]).id).parent = page1;
			treelist.get(page1).childs.add(Pages.get(result[1]).id);
			treelist.get(Pages.get(result[1]).id).parent = page1;
			RefreshRP(page1);

			treelist.get(page2).childs = new ArrayList<Integer>();
			treelist.get(page2).childs.add(Pages.get(result[2]).id);
			treelist.get(Pages.get(result[2]).id).parent = page2;
			treelist.get(page2).childs.add(Pages.get(result[3]).id);
			treelist.get(Pages.get(result[3]).id).parent = page2;
			RefreshRP(page2);
		} else {
			RLeafPage RLP = (RLeafPage)treelist.get(page1);
			RLeafPage RLP2 = (RLeafPage)treelist.get(page2);
			ArrayList<Point> Points = RLP.points;
			Points.add(RLP2.points.get(0));

			for (int i = 0; i < Points.size() - 1; i++) {
				for (int j = i + 1; j < Points.size(); j++) {
					temp1 = new int[2];
					temp2 = new int[2];
					temp1[0] = i;
					temp1[1] = j;
					int m = 0;
					for (int k = 0; k < Points.size(); k++) {
						if (k != i && k != j) {
							temp2[m] = k;
							m++;
							if (m == 2) {
								break;
							}
						}
					}
					Rectangle R1 = new Rectangle(Points.get(temp1[0]), Points.get(temp1[1]));
					Rectangle R2 = new Rectangle(Points.get(temp2[0]), Points.get(temp2[1]));
					float tempinc = increase(R1, R2);
					//System.out.println("tempinc: "+tempinc);
					if (tempinc > inc) {

						inc = tempinc;
						result[0] = temp1[0];
						result[1] = temp1[1];
						result[2] = temp2[0];
						result[3] = temp2[1];

					}
				}
			}
			//System.out.println(result[0]);
			//System.out.println(result[1]);
			//System.out.println(result[2]);
			//System.out.println(result[3]);
			((RLeafPage)treelist.get(page1)).points = new ArrayList<Point>();
			((RLeafPage)treelist.get(page1)).points.add(Points.get(result[0]));
			((RLeafPage)treelist.get(page1)).points.add(Points.get(result[1]));
			((RLeafPage)treelist.get(page1)).refresh();
			((RLeafPage)treelist.get(page2)).points = new ArrayList<Point>();
			((RLeafPage)treelist.get(page2)).points.add(Points.get(result[2]));
			((RLeafPage)treelist.get(page2)).points.add(Points.get(result[3]));
			((RLeafPage)treelist.get(page2)).refresh();
		}
	}

	public float increase(Rectangle R1, Rectangle R2) {
		Rectangle R = R1;
		R.MergeR(R2);
		float inc = R.area();
		if (R1.intersect(R2) == 0) {
			//System.out.println("0!");
			inc = inc - R1.area() - R2.area();
		} else if (R1.intersect(R2) == -1) {
			//System.out.println("intersect!");
			inc = inc - R1.area() - R2.area() + Math.abs(R2.intersect(R1));
		} else {
			//System.out.println("intersect!");
			inc = inc - R1.area() - R2.area() + Math.abs(R1.intersect(R2));
		}

		return Math.abs(inc);
	}


	public void RefreshRP(int id) {
		//System.out.println("id:"+id);
		RPage RP = treelist.get(id);
		treelist.get(id).Refresh(treelist.get(RP.childs.get(0)).getRegion());
		//System.out.println("refresh child: "+RP.childs.get(0));
		//System.out.println("area: "+treelist.get(RP.childs.get(0)).getRegion().area());
		for (int i = 1; i < RP.childs.size(); i++) {
			treelist.get(id).merge(treelist.get(RP.childs.get(i)).getRegion());
			//System.out.println("refresh child: "+RP.childs.get(i));
			//System.out.println("area: "+treelist.get(RP.childs.get(i)).getRegion().area());
		}

	}

	public void beforeFirst(Point p, int key) {
		// TODO Auto-generated method stub
		pointer = -1;
		search(p, key);
		System.out.println("Results: " + resultList.size());
	}

	public boolean next() {
		// TODO Auto-generated method stub
		if (pointer + 1 == resultList.size())
			return false;
		else {
			pointer++;
			return true;
		}
	}

	public RID getDataRid() {
		// TODO Auto-generated method stub
		Point result = resultList.get(pointer);
		return result.getData();
	}

	public int getRoot() {
		// TODO Auto-generated method stub
		return root;
	}

	public HashMap<Integer, RPage> getList() {
		// TODO Auto-generated method stub
		return treelist;
	}



}
