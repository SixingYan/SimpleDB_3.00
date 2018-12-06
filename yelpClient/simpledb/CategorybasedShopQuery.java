import java.sql.*;

import simpledb.remote.SimpleDriver;

/**
*
* @author Xinmeng Gu | Sixing Yan
*/
public class CategorybasedShopQuery {
    public static void main(String[] args) {

		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "SELECT name "
			           + "FROM business, businessLocation, businessCategories"
					   + "WHERE bid=blid AND bcid=id AND "
					   + "categories='food' AND "
					   + "distance() < 10 ";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			while (rs.next()) {
				String bname = rs.getString("name");
				System.out.println(bname);
			}
			rs.close();
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
