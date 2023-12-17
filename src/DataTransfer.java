

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
	    	//2.Lấy cấu hình từ file config.properties
	        Properties properties = loadProperties("config.properties");

	        try {
	        	//4. Kiểm tra bước trước đó có thành công hay không
	        	if (isStagingSuccessful(properties)) {
	        	//5. Ghi log bắt đầu lấy dữ liệu	
	            logger.log(Level.INFO, "Staging to warehouse start.");

	            // Check the log for successful staging
	        	    Class.forName("com.mysql.cj.jdbc.Driver");
	                //6. Kết nối DB staging và DB data_warehouse
	                // Source database connection
	                Connection sourceConn = DriverManager.getConnection(properties.getProperty("source.db.url"),
	                        properties.getProperty("source.db.username"), properties.getProperty("source.db.password"));

	                // Destination database connection
	                Connection destConn = DriverManager.getConnection(properties.getProperty("dest.db.url"),
	                        properties.getProperty("dest.db.username"), properties.getProperty("dest.db.password"));

	                // Transfer data from source to destination
	                //7.
	                transferData(sourceConn, destConn);

	                // 19. Ghi log chuyển dữ liệu thành công
	                logger.log(Level.INFO, "Staging to warehouse completed.");

	                // Close connections
	                sourceConn.close();
	                destConn.close();
	            } else {
	                logger.log(Level.SEVERE, "Staging to warehouse skipped due to previous failure.");
	            }

	        } catch (ClassNotFoundException | SQLException e) {
	            // Log the error
//	        	e.printStackTrace();
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
						"INSERT INTO log (tracking_date_time, source, connect_status, destination, phase, result, detail) VALUES (?, ?, ?, ?, ?, ?, ?)");
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
	private static boolean isStagingSuccessful(Properties properties) {
	    try {
	        // 3. Kết nối DB Control
	        Connection dbConn = DriverManager.getConnection(properties.getProperty("log.db.url"),
	                properties.getProperty("log.db.username"), properties.getProperty("log.db.password"));

	        // Check if there is a successful staging log entry with the current date and highest id
	        PreparedStatement stmt = dbConn.prepareStatement(
	                "SELECT COUNT(*) FROM log WHERE phase = 'csv to staging' AND result = 'Thành công' " +
	                "AND DATE(tracking_date_time) = CURDATE() " +
	                "AND id = (SELECT MAX(id) FROM log)");
	        
	        ResultSet resultSet = stmt.executeQuery();

	        if (resultSet.next()) {
	            int count = resultSet.getInt(1);
	            return count > 0;
	        }

	        // Close resources
	        stmt.close();
	        dbConn.close();

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }

	    // Default to false if there is an exception
	    return false;
	}
	private static void transferData(Connection sourceConn, Connection destConn) throws SQLException {
		
		
		// Transfer data for the province table
		transferProvinceData(sourceConn, destConn);

		// Transfer data for the date table
		transferDateData(sourceConn, destConn);
		
		// Transfer data for the result_lottery table
		transferResultLotteryData(sourceConn, destConn);
	}

	private static void transferResultLotteryData(Connection sourceConn, Connection destConn) throws SQLException {
		// Truy vấn dữ liệu từ cơ sở dữ liệu nguồn
		Statement sourceStmt = sourceConn.createStatement();
		//13. Lấy dữ liệu từ bảng loterry từ DB Staging
		ResultSet resultSet = sourceStmt.executeQuery("SELECT * FROM lottery");

		// Thêm dữ liệu vào cơ sở dữ liệu đích
		PreparedStatement destStmt = destConn.prepareStatement(
				"INSERT INTO result_lottery (prize_name, result, province_id, date_id) VALUES (?, ?, ?, ?)");

		while (resultSet.next()) {
			//14. Xử lý dữ liệu về đúng định dạng
			int id = resultSet.getInt("id");
			String province = resultSet.getString("province");
			String[] prizes = { resultSet.getString("prize_eight"), resultSet.getString("prize_seven"),
					resultSet.getString("prize_six"), resultSet.getString("prize_five"),
					resultSet.getString("prize_four"), resultSet.getString("prize_three"),
					resultSet.getString("prize_two"), resultSet.getString("prize_one"),
					resultSet.getString("prize_special") };
			String date = resultSet.getString("date");
			//15. lấy province_id và date_id
			// Lấy province_id từ cơ sở dữ liệu đích
			int provinceId = getProvinceId(destConn, province);

			// Lấy date_id từ cơ sở dữ liệu đích
			int dateId = getDateId(destConn, date);

			// 16/18
			for (int i = 0; i < prizes.length; i++) {
				//17. Lưu dữ liệu giải theo giá trị i+1
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
		//7. Lấy dữ liệu từ bảng province từ DB staging và xử lý đúng định dạng
		ResultSet resultSet = sourceStmt.executeQuery("SELECT DISTINCT province FROM lottery");

		// Insert data into the destination database
		PreparedStatement destStmt = destConn.prepareStatement("INSERT INTO dim_province (name_province) VALUES (?)");

		while (resultSet.next()) {
			String nameProvince = resultSet.getString("province");

			// 8. Kiểm tra dữ liệu đã có trong dim_province không
			if (!provinceExists(destConn, nameProvince)) {
				//9. Lưu dữ liệu vào dim_province
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
		//10. Lấy dữ liệu từ bảng date từ DB staging và xử lý đúng định dạng
		ResultSet resultSet = sourceStmt.executeQuery("SELECT DISTINCT date FROM lottery");

		// Insert data into the destination database
		PreparedStatement destStmt = destConn
				.prepareStatement("INSERT INTO dim_date ( full_date, day, month, year) VALUES ( ?, ?, ?, ?)");

		while (resultSet.next()) {
			// xử lý đúng định dạng

			String dateStr = resultSet.getString("date");

	        // Chuyển đổi chuỗi thành đối tượng LocalDateTime
	        LocalDateTime dateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_DATE_TIME);
	        
	        // Lấy ngày, tháng, và năm từ đối tượng LocalDateTime
	        int day = dateTime.getDayOfMonth();
	        int month = dateTime.getMonthValue();
	        int year = dateTime.getYear();
			
			// 11. Kiểm tra dữ liệu đã có trong DB data_warehouse không
			if (!dateExists(destConn, dateStr)) {
				//12. Lưu dữ liệu vào dim_date 
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
