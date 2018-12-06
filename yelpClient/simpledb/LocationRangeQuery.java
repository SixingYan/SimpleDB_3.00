import java.sql.*;

import simpledb.query.QPlan;
import simpledb.query.QScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class LocationRangeQuery {
    public static void main(String[] args) {

		Connection conn = null;
		try {
			// Step 1: connect to database server
			//Driver d = new SimpleDriver();
			//conn = d.connect("jdbc:simpledb://localhost", null);
			// analogous to the driver
			SimpleDB.init("studentdb");
						
			// analogous to the connection
			Transaction tx = new Transaction();
						
			// Step 2: execute the query
			//Statement stmt = conn.createStatement();
			String qry = "SELECT businessName "
			           + "FROM business, businessLocation"
			           + "WHERE bid=blid AND "
			           + "city='Las Vegas' AND "
			           + "distance() < 1 AND"
			           + "LIMIT 5";
			
			//ResultSet rs = stmt.executeQuery(qry);
			// analogous to the statement
			boolean noServer = true;
			QPlan p = (QPlan) SimpleDB.planner().createQueryPlan(qry, tx, noServer);
			
			// Step 3: loop through the result set
			// analogous to the result set
			QScan s = p.open(noServer);
			System.out.println();
			System.out.println("=================Start=================");
			System.out.println("BusinessName");
			System.out.println("________________________");
			while (s.next()) {
				String sname = s.getString("sname"); //SimpleDB stores field names, all in lower case
				System.out.println(sname);
			}
			System.out.println("=================End===================");
			System.out.println();
			
			s.close();
			tx.commit();
				
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			// Step 4: close the connection
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
