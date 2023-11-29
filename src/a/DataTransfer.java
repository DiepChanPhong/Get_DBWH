package a;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class DataTransfer {
	private static final Logger logger = Logger.getLogger(DataTransfer.class.getName());

	static {
		Handler dbHandler = new DatabaseHandler();
		logger.addHandler(dbHandler);
	}

	public static void main(String[] args) {
		Properties properties = loadProperties("config.properties");

		try {
			logger.log(Level.INFO, "Staging to warehouse start.");
			
			// Load the MySQL JDBC driver
			Class.forName("com.mysql.cj.jdbc.Driver");

			// Source database connection
			Connection sourceConn = DriverManager.getConnection(properties.getProperty("source.db.url"),
					properties.getProperty("source.db.username"), properties.getProperty("source.db.password"));

			// Destination database connection
			Connection destConn = DriverManager.getConnection(properties.getProperty("dest.db.url"),
					properties.getProperty("dest.db.username"), properties.getProperty("dest.db.password"));

			// Transfer data from source to destination
			transferData(sourceConn, destConn);

			// Log the end of the code
			logger.log(Level.INFO, "Staging to warehouse completed.");

			// Close connections
			sourceConn.close();
			destConn.close();

		} catch (ClassNotFoundException | SQLException e) {
			// Log the error
			logger.log(Level.SEVERE, "Staging to warehouse failed.", e);
		}
	}

	private static Properties loadProperties(String filePath) {
		Properties properties = new Properties();
		try (FileInputStream input = new FileInputStream(filePath)) {
			properties.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	private static class DatabaseHandler extends Handler {
		@Override
		public void publish(LogRecord record) {
			try {
				// Set up your database connection
				Properties properties = loadProperties("config.properties");
				Connection dbConn = DriverManager.getConnection(properties.getProperty("log.db.url"),
						properties.getProperty("log.db.username"), properties.getProperty("log.db.password"));
				// Insert log record into the database
				PreparedStatement stmt = dbConn.prepareStatement(
						"INSERT INTO log (tracking_date, source, connect_status, destination, phase, result, detail) VALUES (?, ?, ?, ?, ?, ?, ?)");
				stmt.setTimestamp(1, new Timestamp(record.getMillis()));
				stmt.setString(2, "staging.db.lottery");
				stmt.setInt(3, record.getLevel() == Level.INFO ? 1 : 0);
				stmt.setString(4, "warehouse.db.lottery");
				stmt.setString(5, "staging to warehouse");
				stmt.setString(6, record.getLevel() == Level.INFO ? "Thành công" : "Thất bại");
				stmt.setString(7, record.getMessage());

				stmt.executeUpdate();

				// Close resources
				stmt.close();
				dbConn.close();

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void flush() {
			// Not needed for this example
		}

		@Override
		public void close() throws SecurityException {
			// Not needed for this example
		}
	}

	private static void transferData(Connection sourceConn, Connection destConn) throws SQLException {
		// Transfer data for the result_lottery table
		transferResultLotteryData(sourceConn, destConn);

		// Transfer data for the province table
		transferProvinceData(sourceConn, destConn);

		// Transfer data for the date table
		transferDateData(sourceConn, destConn);
	}

	private static void transferResultLotteryData(Connection sourceConn, Connection destConn) throws SQLException {
		// Truy vấn dữ liệu từ cơ sở dữ liệu nguồn
		Statement sourceStmt = sourceConn.createStatement();
		ResultSet resultSet = sourceStmt.executeQuery("SELECT * FROM lottery");

		// Thêm dữ liệu vào cơ sở dữ liệu đích
		PreparedStatement destStmt = destConn.prepareStatement(
				"INSERT INTO result_lottery (prize_name, result, province_id, date_id) VALUES (?, ?, ?, ?)");

		while (resultSet.next()) {
			int id = resultSet.getInt("id");
			String province = resultSet.getString("province");
			String[] prizes = { resultSet.getString("prize_eight"), resultSet.getString("prize_seven"),
					resultSet.getString("prize_six"), resultSet.getString("prize_five"),
					resultSet.getString("prize_four"), resultSet.getString("prize_three"),
					resultSet.getString("prize_two"), resultSet.getString("prize_one"),
					resultSet.getString("prize_special") };
			String date = resultSet.getString("date");

			// Lấy province_id từ cơ sở dữ liệu đích
			int provinceId = getProvinceId(destConn, province);

			// Lấy date_id từ cơ sở dữ liệu đích
			int dateId = getDateId(destConn, date);

			// Thêm dữ liệu vào cơ sở dữ liệu đích cho từng giải
			for (int i = 0; i < prizes.length; i++) {
				destStmt.setString(1, "prize " + (i + 1)); // Giả sử tên giải là "Giải 1", "Giải 2", ...
				destStmt.setString(2, prizes[i]);
				destStmt.setInt(3, provinceId);
				destStmt.setInt(4, dateId);

				destStmt.executeUpdate();
			}
		}

		// Đóng các câu lệnh
		sourceStmt.close();
		destStmt.close();

	}

	private static int getProvinceId(Connection destConn, String province) throws SQLException {
		PreparedStatement stmt = destConn.prepareStatement("SELECT id FROM dim_province WHERE name_province = ?");
		stmt.setString(1, province);
		ResultSet resultSet = stmt.executeQuery();

		if (resultSet.next()) {
			return resultSet.getInt("id");
		} else {
			// Handle case where province does not exist in the destination database
			return -1;
		}
	}

	private static int getDateId(Connection destConn, String date) throws SQLException {
		PreparedStatement stmt = destConn.prepareStatement("SELECT id FROM dim_date WHERE full_date = ?");
		stmt.setString(1, date);
		ResultSet resultSet = stmt.executeQuery();

		if (resultSet.next()) {
			return resultSet.getInt("id");
		} else {
			// Handle case where date does not exist in the destination database
			return -1;
		}
	}

	private static void transferProvinceData(Connection sourceConn, Connection destConn) throws SQLException {
		// Retrieve data from the source database
		Statement sourceStmt = sourceConn.createStatement();
		ResultSet resultSet = sourceStmt.executeQuery("SELECT DISTINCT province FROM lottery");

		// Insert data into the destination database
		PreparedStatement destStmt = destConn.prepareStatement("INSERT INTO dim_province (name_province) VALUES (?)");

		while (resultSet.next()) {
			String nameProvince = resultSet.getString("province");

			// Check if province already exists in the destination database
			if (!provinceExists(destConn, nameProvince)) {
				destStmt.setString(1, nameProvince);
				destStmt.executeUpdate();
			}
		}

		// Close statements
		sourceStmt.close();
		destStmt.close();
	}

	private static boolean provinceExists(Connection destConn, String province) throws SQLException {
		PreparedStatement stmt = destConn.prepareStatement("SELECT id FROM dim_province WHERE name_province = ?");
		stmt.setString(1, province);
		ResultSet resultSet = stmt.executeQuery();

		return resultSet.next();
	}

	private static void transferDateData(Connection sourceConn, Connection destConn) throws SQLException {
		// Retrieve data from the source database
		Statement sourceStmt = sourceConn.createStatement();
		ResultSet resultSet = sourceStmt.executeQuery("SELECT DISTINCT date FROM lottery");

		// Insert data into the destination database
		PreparedStatement destStmt = destConn
				.prepareStatement("INSERT INTO dim_date (id, full_date, day, month, year) VALUES (?, ?, ?, ?, ?)");

		while (resultSet.next()) {
			String dateStr = resultSet.getString("date");

			// Assuming the date format is "dd/MM/yyyy"
			String[] dateParts = dateStr.split("/");
			int day = Integer.parseInt(dateParts[0]);
			int month = Integer.parseInt(dateParts[1]);
			int year = Integer.parseInt(dateParts[2]);

			// Check if date already exists in the destination database
			if (!dateExists(destConn, dateStr)) {
				destStmt.setString(1, dateStr);
				destStmt.setInt(2, day);
				destStmt.setInt(3, month);
				destStmt.setInt(4, year);

				destStmt.executeUpdate();
			}
		}

		// Close statements
		sourceStmt.close();
		destStmt.close();
	}

	private static boolean dateExists(Connection destConn, String date) throws SQLException {
		PreparedStatement stmt = destConn.prepareStatement("SELECT id FROM dim_date WHERE full_date = ?");
		stmt.setString(1, date);
		ResultSet resultSet = stmt.executeQuery();

		return resultSet.next();
	}
}
