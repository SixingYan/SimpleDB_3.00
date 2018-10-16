import java.sql.*;
import simpledb.remote.SimpleDriver;
/**
 * Create an index on MajorId field of table Student.
 * @author Sixing Yan
 */
public class CreateMajorIndex {
	public static void main(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();
			String s = "create index MAJORID_INDEX on STUDENT(MajorId)";
			stmt.executeUpdate(s);
			System.out.println("Index MAJORID_INDEX created.");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
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
