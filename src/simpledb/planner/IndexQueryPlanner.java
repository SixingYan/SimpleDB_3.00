package simpledb.planner;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.parse.*;
import simpledb.server.SimpleDB;
import java.util.*;

public class IndexQueryPlanner implements QueryPlanner {
	
	/**
	* Creates a query plan as follows.  It first takes
	* the product of all tables and views; it then selects on the predicate;
	* and finally it projects on the field list.
	*/
	public Plan createPlan(QueryData data, Transaction tx) {
		//Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
			else
				plans.add(new TablePlan(tblname, tx));
		}

		//Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);

		//Step 3: Add a selection plan for the predicate
		IndexFinder ifder = new IndexFinder(data);
		
		for (String tblname : data.tables()) {
			Map<String, IndexInfo> iiMap = SimpleDB.mdMgr().getIndexInfo(tblname, tx);
			if (iiMap.isEmpty()) continue;
			for (String fldname: iiMap.keySet()) {
				// search for terms like F1 * c, e.g., F1 > c
				Constant cst = data.pred().operatesWithConstant(fldname);
				if (cst != null)
					ifder.addConstant(tblname, fldname, cst);
				// search for terms like F1 * F2, e.g., F1 = F2
				String fld = data.pred().operatesWithField(fldname);
				if (fld != null)
					ifder.addField(tblname, fldname, fld);
			}
		}
		
		p = ifder.hasIndexInfo() ? new IndexSelectPlan(p, ifder.getIndexInfo, ifder.getSearchKey(), tx) :new SelectPlan(p, data.pred());

		//Step 4: Project on the field names
		p = new ProjectPlan(p, data.fields());
		return p;
	}
}
