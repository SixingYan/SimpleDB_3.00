package simpledb.planner;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.index.IndexFinder;
import simpledb.index.query.IndexSelectPlan;
import simpledb.materialize.GroupByPlan;
import simpledb.parse.*;
import simpledb.server.SimpleDB;
import java.util.*;

/**
 * The advanced query planner, supporting index and group by.
 * @author Sixing Yan
 */

public class AdvQueryPlanner {
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
		IndexFinder ifder = new IndexFinder(data, tx);
		p = ifder.hasIndexInfo() ? new IndexSelectPlan(p, ifder.getIndexInfo(), ifder.getSearchKey(), tx) : new SelectPlan(p, data.pred());

		//Step 4: GroupBy (also Project) on the field names
		p = data.groupfields() != null ? new GroupByPlan(p, data.groupfields(), data.aggfns(), tx) : new ProjectPlan(p, data.fields());

		return p;
	}
}

