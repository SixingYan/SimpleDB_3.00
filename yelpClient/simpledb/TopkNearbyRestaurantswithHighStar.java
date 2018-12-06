import java.sql.*;
import simpledb.remote.SimpleDriver;

public class TopkNearbyRestaurantswithHighStar {
    public static void main(String[] args) {


		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "SELECT businessId "
			           + "FROM business, businessLocation "
			           + "WHERE distance()<Inputdist AND stars>4 AND businessId=businessId";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			while (rs.next()) {
				String bid = rs.getString("businessId");
				System.out.println(bid);
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
