import java.sql.*;
import simpledb.remote.SimpleDriver;

public class ShopEntityQuery {
    public static void main(String[] args) {


		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "CREATE VIEW countcheckins AS "
			           + "SELECT COUNT(CI.checkins) AS count, CI.businessId "
					   + "FROM checkIn CI, business B "
			           + "WHERE B.businessId=CI.businessId "
					   + "GROUP BY CI.businessId";
			stmt.executeUpdate(qry);
			
			qry = "SELECT B.businessName, B.isOpen, B.stars, C.count "
				+ "FROM count C, business B "
				+ "WHERE B.businessId=C.businessId AND B.businessName=inputbusinessName";
			ResultSet rs = stmt.executeQuery(qry);
			
			// Step 3: loop through the result set
			while (rs.next()) {
				String bname = rs.getString("businessName");
				int isOpen = rs.getInt("isOpen");
				int stars = rs.getInt("stars");
				int count = rs.getInt("count");
				System.out.println(bname + "\t" + isOpen + "\t" + stars + "\t" + count);
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
