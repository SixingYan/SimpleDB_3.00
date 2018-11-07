package simpledb.index.rtree;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class RTreeIndex implements Index {
	
	private HashMap<Integer,RPage> treelist = new HashMap<Integer,RPage>();
	private int root;
	private int isnull;
	static final float Ruler=3;
	static final int Limit1=4;
	static final int Limit2=4;
	static final int DownLimit1=2;
	static final int DownLimit2=2;
	private ArrayList<Point> resultList=new ArrayList<Point>();
	private ArrayList<RPage> deleteList=new ArrayList<RPage>();
	private ArrayList<Point> PointWaitingList=new ArrayList<Point>();
	private ArrayList<RPage> PageWaitingList=new ArrayList<RPage>();
	private int depth;
	private int Chose;
	private int idGiver;
	private int checkdepth;
	
	public RTreeIndex() {
		idGiver=0;
		RPage Root=NewRPage();
		treelist.put(Root.id, Root);
		root=0;
		depth=0;
		isnull=1;
	}
	
	public RTreeIndex (String idxname, Schema sch, Transaction tx) {}
	
	public RPage NewRPage() {
		RPage NewPage=new RPage();
		NewPage.id=idGiver;
		idGiver++;
		return NewPage;
	}
	

	
	public ArrayList<Point> search(Point UserP) {
		resultList=new ArrayList<Point>();
		Rectangle searchR=new Rectangle(UserP,Ruler,Ruler);
		searchlevel(treelist.get(root),searchR);
		
		return resultList;
	}
	
	public void searchlevel(RPage RP,Rectangle searchR) {
		for(int i=0;i<RP.childs.size();i++) {
			int child=RP.childs.get(i);
			if(treelist.get(child).getClass()==RLeafPage.class) {
				RLeafPage RLP=(RLeafPage)treelist.get(child);
				if(RLP.overlaps(searchR)) {
					for(int j=0;j<RLP.points.size();j++)
					{
						resultList.add(RLP.points.get(j));
					}
				}
			}
			else
			{
				RPage ChildRP =(RPage)treelist.get(child);
				if(ChildRP.overlaps(searchR)) {
					searchlevel(ChildRP,searchR);
				}
			}
		}

	}
	
	public void insertPage(RPage RP) {
		checkdepth=1;
		findPlace(treelist.get(root),RP);
		RPage TargetPage=treelist.get(Chose);
		RPage ParentPage=treelist.get(TargetPage.parent);
		if(ParentPage.childs.size()>Limit1) {
			RPage NewPage=new RPage();
			NewPage.Refresh(TargetPage.getRegion());
			NewPage.id=idGiver;
			idGiver++;
			NewPage.parent=ParentPage.parent;
			treelist.put(NewPage.id,NewPage);
			Adjusting(treelist.get(NewPage.parent),1);
		}
		else
		{
			treelist.get(ParentPage.id).merge(TargetPage.getRegion());
			Adjusting(treelist.get(Chose),0);
		}
	}
	
	public void findPlace(RPage RP,RPage searchR) {
		if(checkdepth==searchR.getLevel()) {
			if(RP.contains(searchR.getRegion())) {
				searchR.parent=RP.id;
				treelist.get(RP.id).childs.add(RP.id);
				treelist.put(searchR.id,searchR);
			}
		}
		else
		{
			for(int i=0;i<RP.childs.size();i++) {
				RPage ChildPage=treelist.get(RP.childs.get(i));
				if(ChildPage.contains(searchR.getRegion())) {
					checkdepth++;
					findPlace(ChildPage,searchR);
					break;
				}
			}
		}
	}
	
	public void insert(Point RestP) {
		if(isnull==1)
		{
			RLeafPage NewLeaf=new RLeafPage(RestP,Ruler,Ruler);
			NewLeaf.id=idGiver;
			idGiver++;
			NewLeaf.parent=root;
			treelist.put(NewLeaf.id,NewLeaf);
			treelist.get(root).childs.add(NewLeaf.id);
			depth++;
			isnull=0;
			return;
			
		}
		chooseLeaf(treelist.get(root),RestP);
		RLeafPage TargetPage=(RLeafPage)treelist.get(Chose);
		if(TargetPage.points.size()>Limit2) {
			RLeafPage NewPage=new RLeafPage(RestP,Ruler,Ruler);
			NewPage.id=idGiver;
			idGiver++;
			NewPage.parent=TargetPage.parent;
			treelist.put(NewPage.id,NewPage);
			Adjusting(NewPage,1);
		}
		else
		{
			((RLeafPage)treelist.get(Chose)).merge(RestP);
			Adjusting(treelist.get(Chose),0);
		}
		
		
	}
	
	public void Adjusting(RPage NewRP, int flag) {
		
		if(flag==0)
		{
			treelist.get(NewRP.parent).getRegion().MergeR(NewRP.getRegion());
			if(NewRP.parent==root)
			{
				return;
			}
			else
			{
				Adjusting(treelist.get(NewRP.parent),0);
			}
		}
		else
		{
			if(treelist.get(NewRP.parent).childs.size()==Limit1) {
				if(NewRP.parent==root) {
					RPage OFRP=NewRPage();
					OFRP.childs.add(NewRP.id);
					OFRP.getRegion().MergeR(NewRP.getRegion());
					RPage NewRoot=NewRPage();
					NewRoot.childs.add(OFRP.id);
					NewRoot.childs.add(NewRP.parent);
					OFRP.parent=NewRoot.id;
					NewRoot.getRegion().MergeR(OFRP.getRegion());
					NewRoot.getRegion().MergeR(treelist.get(NewRP.parent).getRegion());
					treelist.get(NewRP.parent).parent=NewRoot.id;
					treelist.put(OFRP.id,OFRP);
					treelist.put(NewRoot.id,NewRoot);
					root=NewRoot.id;
					depth++;
					return;
				}
				else
				{
					RPage OFRP=NewRPage();
					OFRP.childs.add(NewRP.id);
					OFRP.getRegion().MergeR(NewRP.getRegion());
					treelist.get(NewRP.id).parent=OFRP.id;
					treelist.put(OFRP.id,OFRP);
					Adjusting(OFRP,1);
				}

			}
			else
			{
				if(NewRP.parent==root) {
					treelist.get(NewRP.parent).childs.add(NewRP.id);
					treelist.get(NewRP.parent).getRegion().MergeR(NewRP.getRegion());
					return;
				}
				else
				{
					treelist.get(NewRP.parent).childs.add(NewRP.id);
					treelist.get(NewRP.parent).getRegion().MergeR(NewRP.getRegion());
					Adjusting(treelist.get(NewRP.parent),0);
				}

			}
		}
		
	}
	
	public void chooseLeaf(RPage RP,Point P) {
		float cost=0;
		int flag=0;
		int target=0;
		for(int i=0;i<RP.childs.size();i++) {
			int child=RP.childs.get(i);
			if(treelist.get(child).getClass()==RLeafPage.class) {
				flag=1;
				RLeafPage RLP=(RLeafPage)treelist.get(child);
				if(RLP.getRegion().contain(P.getX(),P.getY())) {
					Chose=child;
					return;
				}
				else
				{
					if(cost==0) {
						cost=RLP.getRegion().cost(P);
						target=child;
					}
					else
					{
						if(cost>RLP.getRegion().cost(P)) {
							cost=RLP.getRegion().cost(P);
							target=child;
							
						}
					}
				}
			}
			else
			{
				RPage ChildRP =(RPage)treelist.get(child);
				if(ChildRP.getRegion().contain(P.getX(),P.getY())) {
					chooseLeaf(ChildRP,P);
				}
				else
				{
					if(cost==0) {
						cost=ChildRP.getRegion().cost(P);
						target=child;
					}
					else
					{
						if(cost>ChildRP.getRegion().cost(P)) {
							cost=ChildRP.getRegion().cost(P);
							target=child;
						}
					}
				}
			}
		}
		
		if(flag==0)
		{
			chooseLeaf(treelist.get(target),P);
		}
		else
		{
			Chose=target;
		}

	}
	
	
	public void delete(Point RestP) {
		resultList=new ArrayList<Point>();
		PointWaitingList=new ArrayList<Point>();
		deletesearching(treelist.get(root),RestP);
		
		for(int i=0;i<deleteList.size();i++) {
			checkdepth=depth;
			RLeafPage RLP=(RLeafPage)deleteList.get(i);
			if(RLP.points.size()==DownLimit2) {
				for(int j=0;j<RLP.points.size();j++) {
					
					if(!(RLP.points.get(j).Same(RestP))) {
						PointWaitingList.add(RLP.points.get(j));
					}
				}
				
				treelist.remove(RLP.id);
				RPage ParentR=treelist.get(RLP.parent);
				for(int j=0;j<ParentR.childs.size();j++)
				{
					if(ParentR.childs.get(j)==RLP.id) {
						treelist.get(RLP.parent).childs.remove(j);
						break;
					}
				}
				depth--;
				LevelDelete(RLP.parent,0);

			}
			else
			{
				
				for(int j=0;j<RLP.points.size();j++)
				{
					if(RLP.points.get(j).Same(RestP)) {
						((RLeafPage)treelist.get(RLP.id)).points.remove(j);
						j--;
					}
					
				}
				
				((RLeafPage)treelist.get(RLP.id)).refresh();
				LevelDelete(RLP.parent,0);
				
			}
		}
		
		for(int i=0;i<PointWaitingList.size();i++) {
			insert(PointWaitingList.get(i));
		}
		for(int i=0;i<PageWaitingList.size();i++) {
			insertPage(PageWaitingList.get(i));
		}
	}
	
	public void deletesearching(RPage RP, Point SearchP) {
		for(int i=0;i<RP.childs.size();i++) {
			int child=RP.childs.get(i);
			if(treelist.get(child).getClass()==RLeafPage.class) {
				RLeafPage RLP=(RLeafPage)treelist.get(child);
				if(RLP.getRegion().contain(SearchP.getX(),SearchP.getY())) {
					for(int j=0;j<RLP.points.size();j++)
					{
						if(RLP.points.get(j).Same(SearchP)) {
							deleteList.add(RLP);
						}

					}
				}
			}
			else
			{
				RPage ChildRP =(RPage)treelist.get(child);
				if(ChildRP.getRegion().contain(SearchP.getX(),SearchP.getY())) {
					deletesearching(ChildRP,SearchP);
				}
			}
		}
	}
	
	public void LevelDelete(int Deletedid,int flag) {
		if(flag==0) {
			RPage DeletePage=treelist.get(Deletedid);
			treelist.get(Deletedid).Refresh(treelist.get(DeletePage.childs.get(0)).getRegion());
			for(int i=1;i<DeletePage.childs.size();i++){
				treelist.get(Deletedid).getRegion().MergeR(treelist.get(DeletePage.childs.get(0)).getRegion());
			}
			if(Deletedid==root) {
				return;
			}
			else
			{
				LevelDelete(DeletePage.parent,0);
			}
		}
		else
		{
			RPage DeletePage=treelist.get(Deletedid);
			if(DeletePage.childs.size()<DownLimit1) {
				
				if(DeletePage.id==root) {
					return;
				}
				else
				{
					DeletePage.setLevel(checkdepth);
					PageWaitingList.add(DeletePage);
					treelist.remove(DeletePage.id);
					RPage ParentR=treelist.get(DeletePage.parent);
					for(int j=0;j<ParentR.childs.size();j++)
					{
						if(ParentR.childs.get(j)==DeletePage.id) {
							treelist.get(DeletePage.parent).childs.remove(j);
							break;
						}
					}
					checkdepth--;
					LevelDelete(DeletePage.parent,1);
				}


			}
			else
			{
				
				LevelDelete(Deletedid,0);
				
			}
		}
	}

	@Override
	public void beforeFirst(Constant searchkey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean next() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RID getDataRid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insert(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
