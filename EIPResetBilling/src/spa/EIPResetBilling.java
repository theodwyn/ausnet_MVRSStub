package spa;

import org.apache.log4j.Logger;

import database.EIP_DAO;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;

public class EIPResetBilling {

	private static String propertiesFilename;
	private static String requestsFilename;

	private static Logger log = Logger.getLogger("EIPResetBilling");

	private static EIP_DAO dao;

	private static Request req = null;

	private static double totalRecords = 0;
	private static double recordCount = 0;
	private static double requestCount = 0;
	
	private final static double batchLimit = 20;
	private final static int progressBatch = 10000;

	static RuntimeProperties properties = RuntimeProperties.getInstance();

	public static void main(String[] args) throws InterruptedException, IOException {

		log.info("Start EIP Reset Billing");

		initialize(args);
		dao = new EIP_DAO();

		ReadRequests requests = new ReadRequests(requestsFilename);
		totalRecords = requests.getRequestsCount();

		req = requests.getNextRequest();

		while (req != null) {
			recordCount++;

			// Only generate Billing Requests for MRIM SDP's
			// All SDP's will have one "91" Channel
			if (req.getChannelNumber().equals("91")) {
				if (req.getInstallationType().equals("MRIM")) {
					requestCount++;
					
					// A Billing Request will be created for each Product in the ddProducts property
					// MDMT,1-27HL;NEM12 LR,1-27KH;NEM12 FRMP,1-27J1;PV2,1-27NB
					dao.insertBILL_REQUEST(req);
				}
			}
			// Update all channels (MRIM and BASIC)
			dao.updateLAST_INTERVAL_SET(req);

			// Inserts and Updates batched - execute when defined limit reached
			if (requestCount == batchLimit) {
				dao.executeBatch();
				requestCount = 0;
			}
			
			// Periodically log a progress message
			if (recordCount % progressBatch == 0) {
				progressLogging(req.toString());
			}

			req = requests.getNextRequest();
		}

		progressLogging("Complete");

		dao.executeBatch();
		dao.closeConnection();

		log.info("End EIP Reset Billing");
	}

	// Load runtime properties
	private static void initialize(String[] args) {

		// Retrieve default runtime properties
		propertiesFilename = "EIPResetBilling.properties";
		try {
			properties.load(new FileInputStream(propertiesFilename));
		} catch (Exception e) {
			log.error(".. Error accessing properties file < " + propertiesFilename + " >");
			System.exit(-1);
		}

		requestsFilename = properties.getProperty("requestsFilename");
		log.info(".. Requests filename   : " + requestsFilename);

		try {
			log.info(".. Host                : " + InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e2) {
			log.info(".. Host                : Unknown");
		}
	}

	private static void progressLogging(String message) {
		NumberFormat pctFormat = NumberFormat.getPercentInstance();
		pctFormat.setMinimumFractionDigits(2);

		log.info(String.format(".. Progress : %s - %s",
				pctFormat.format(recordCount / totalRecords), message));
	}
}
