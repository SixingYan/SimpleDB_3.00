import java.sql.*;
import simpledb.remote.SimpleDriver;

/**
 * 
 * @author Xinmeng Gu
 */
public class TopkExcellentShops {
    public static void main(String[] args) {


		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "CREATE VIEW usefuluser AS "
			           + "SELECT MAX(U.useful) AS mostuseful "
					   + "FROM user U "
			           + "GROUP BY U.userid ";
			stmt.executeUpdate(qry);
			
			qry = "SELECT R.businessId "
				+ "FORM review R, usefuluser UU, user U "
				+ "WHERE R.userId=U.userId AND U.fans=UU.mostuseful "
				+ "GROUP BY R.businessId";
			ResultSet rs = stmt.executeQuery(qry);
			
			// Step 3: loop through the result set
			while (rs.next()) {
				String businessid = rs.getString("businessid");
				System.out.println(businessid);
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
