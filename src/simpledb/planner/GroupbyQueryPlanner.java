package simpledb.planner;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.GroupByPlan;
import simpledb.parse.QueryData;
import simpledb.query.Plan;
import simpledb.query.ProductPlan;
import simpledb.query.ProjectPlan;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * 
 * @author Sixing Yan
 */
public class GroupbyQueryPlanner implements QueryPlanner {
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
		p = new SelectPlan(p, data.pred());
		
		//Step 4: GroupBy (also Project) on the field names. GroupBy will create a new 
		p = new GroupByPlan(p, data.groupfields(), data.aggfns(), tx);
		return p;
	}
}
