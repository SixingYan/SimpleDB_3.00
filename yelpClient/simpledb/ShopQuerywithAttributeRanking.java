import java.sql.*;
import simpledb.remote.SimpleDriver;

public class ShopQuerywithAttributeRanking {
    public static void main(String[] args) {


		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "CREATE VIEW highstarreview AS "
			           + "SELECT R.businessId as businessId "
					   + "FROM review R "
			           + "WHERE R.stars>4 AND R.userId=inputuserId "
					   + "GROUP BY R.businessId";
			stmt.executeUpdate(qry);
			
			qry = "CREATE VIEW favourAttrs AS "
			           + "SELECT attribute "
					   + "FROM businessAttributes, highstarreview "
			           + "WHERE businessAttributes.businessId = highstarreview.businessId";
			stmt.executeUpdate(qry);
			
			
			qry = "CREATE VIEW businessWithFavourAttrs AS "
			           + "SELECT businessId, count(attribute) as count "
					   + "FROM businessAttributes, favourAttrs "
			           + "WHERE businessAttributes.attribute = favouriteAttributes.attribute "
					   + "GROUP BY businessAttributes.businessId "
			           + "ORDER BY count "
					   + "LIMIT InputLimit";
			stmt.executeUpdate(qry);
			
			
			qry = "SELECT * "
				+ "FROM business, businessWithFavourAttrs "
				+ "WHERE business.businessId=businessWithFavourAttrs.businessId";
			ResultSet rs = stmt.executeQuery(qry);
			
			// Step 3: loop through the result set
			while (rs.next()) {
				String bname = rs.getString("businessName");
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
