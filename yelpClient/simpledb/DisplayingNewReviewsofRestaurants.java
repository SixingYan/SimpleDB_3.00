import java.sql.*;
import simpledb.remote.SimpleDriver;

public class DisplayingNewReviewsofRestaurants {
    public static void main(String[] args) {


		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "CREATE OR REPLACE VIEW restaurantbeen AS "
			           + "SELECT R, businessId, MAX(R.date) AS newest "
					   + "FROM review R "
			           + "WHERE userId='test' "
					   + "GROUP BY businessId";
			stmt.executeUpdate(qry);
			
			qry = "SELECT R.business, R.stars, R.useful, R.funny, R.cool "
				+ "FROM review R, restaurantbeen RB "
				+ "WHERE RB.businessId=R.businessId AND R.date=RB.newest";
			ResultSet rs = stmt.executeQuery(qry);
			
			// Step 3: loop through the result set
			while (rs.next()) {
				String business = rs.getString("business");
				int star = rs.getInt("stars");
				int useful = rs.getInt("useful");
				int funny = rs.getInt("funny");
				int cool = rs.getInt("cool");
				System.out.println(business + "\t" + star + "\t" + useful + "\t" + funny + "\t" + cool);
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
