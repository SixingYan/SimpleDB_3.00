import java.sql.*;
import simpledb.remote.SimpleDriver;

public class DisplayingNearbyRestaurants {
    public static void main(String[] args) {


		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "CREATE VIEW MAXINCATEGORIES AS "
			           + "SELECT BC.categories, MAX(B.stars) AS hstar "
					   + "FROM business B, businessCategories BC "
			           + "WHERE B.businessId=BC.businessId "
					   + "GROUP BY BC.categories";
			stmt.executeUpdate(qry);
			
			qry = "SELECT B.businessName, B.businessId "
				+ "FROM business B, MAXINCATEGORIES MIC "
				+ "WHERE B.stars=MIC.hstar";
			ResultSet rs = stmt.executeQuery(qry);
			
			// Step 3: loop through the result set
			while (rs.next()) {
				String businessName = rs.getString("businessName");
				String businessid = rs.getString("businessid");
				System.out.println(businessName + "\t" + businessid);
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
