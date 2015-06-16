package database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DbUtil {
	public static void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) { /* log or print or ignore */
			}
		}
	}

	public static void close(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) { /* log or print or ignore */
			}
		}
	}

	public static void close(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) { /* log or print or ignore */
			}
		}
	}
	
	public static String formatTimestampToString(Timestamp ts) {
		SimpleDateFormat sdf = new SimpleDateFormat(
				"dd/MMM/yy HH:mm:ss.000000000");
		String result = null;
		if (ts != null) {
			result = sdf.format(ts);
		}
		return result;
	}
	
	public static Timestamp formatStringToTimestamp(String ts) {
		SimpleDateFormat sdf = new SimpleDateFormat(
				"dd/MMM/yy HH:mm:ss.000000000");
		Timestamp result = null;
		
		try {
			Date parseDate = sdf.parse(ts);
			result = new Timestamp(parseDate.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static Timestamp addSecondsToTimestamp(Timestamp 
			ts, int seconds) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(ts.getTime());
		cal.add(Calendar.SECOND, seconds);
		return new Timestamp(cal.getTime().getTime());
	}
}