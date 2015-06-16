package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import spa.RuntimeProperties;

public class ConnectionFactory {

	private static String app;

	private static RuntimeProperties properties = RuntimeProperties.getInstance();

	// static reference to itself
	private static ConnectionFactory instance = new ConnectionFactory();
	
	private String system;

	private String URL;
	private String USER;
	private String PASSWORD;
	private String DRIVER_CLASS;

	private Connection createConnection() {

		Connection connection = null;
		try {
			connection = DriverManager.getConnection(URL, USER, PASSWORD);
		} catch (SQLException e) {
			System.out.println("ERROR: Unable to Connect to Database.");
		}
		return connection;
	}

	public Connection getConnection(String app) {
		this.system = app;
		
		switch (AmiSystem.valueOf(system)) {
		case EIP:
			instance.URL = properties.getProperty("EIPURL");
			instance.USER = properties.getProperty("EIPUSER");
			instance.PASSWORD = properties.getProperty("EIPPASSWORD");
			instance.DRIVER_CLASS = properties.getProperty("EIPDRIVERCLASS");
			break;
		case CIS:
			instance.URL = properties.getProperty("CISURL");
			instance.USER = properties.getProperty("CISUSER");
			instance.PASSWORD = properties.getProperty("CISPASSWORD");
			instance.DRIVER_CLASS = properties.getProperty("CISDRIVERCLASS");
			break;
		case PN:
			instance.URL = properties.getProperty("PNURL");
			instance.USER = properties.getProperty("PNUSER");
			instance.PASSWORD = properties.getProperty("PNPASSWORD");
			instance.DRIVER_CLASS = properties.getProperty("PNDRIVERCLASS");
			break;
		}
		
		try {
			Class.forName(instance.DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return instance.createConnection();
	}

	public static String getApp() {
		return app;
	}

	public void setApp(String app) {
		ConnectionFactory.app = app;
	}

	public enum AmiSystem {
		EIP, CIS, PN
	}
}
