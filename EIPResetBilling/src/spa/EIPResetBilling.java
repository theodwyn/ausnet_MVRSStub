package spa;

import org.apache.log4j.Logger;

import database.EIP_DAO;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;

public class EIPResetBilling {

	private static String propertiesFilename;
	private static String requestsFilename;

	private static Logger log = Logger.getLogger("EIPResetBilling");

	private static EIP_DAO dao;

	private static Request req = null;

	private static double totalRecords = 0;
	private static double recordCount = 0;
	private static double requestCount = 0;

	private static int batchSize = 20;
	private static int batchProgress = 10000;

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

					// A Billing Request will be created for each Product in the
					// ddProducts property (MDMT;NEM12 LR;NEM12 FRMP;PV2)
					dao.insertBILL_REQUEST(req);
				}

				// Update references for active DD services for the current SDP
				List<String> DDSlist = dao.listDDS(req.getSDPRef());

				Iterator<String> it = DDSlist.iterator();
				while (it.hasNext()) {
					dao.updateLAST_INTERVAL_SET(it.next());
				}
			}

			// Inserts and Updates batched - execute when defined limit reached
			if (requestCount == batchSize) {
				dao.executeBatch();
				requestCount = 0;
			}

			// Periodically log a progress message
			if (recordCount % batchProgress == 0) {
				progressLogging(req.toString());
			}

			req = requests.getNextRequest();
		}

		// Flush any remaining inserts/updates
		dao.executeBatch();
		dao.closeConnection();

		progressLogging("Complete");
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

		String propertyValue = null;

		properties.getProperty("batchSize");
		try {
			batchSize = Integer.parseInt(propertyValue);
		} catch (NumberFormatException e) {
			batchSize = 20;
		}
		log.info(".. Oracle BatchSize    : " + batchSize);

		propertyValue = properties.getProperty("batchProgress");
		try {
			batchProgress = Integer.parseInt(propertyValue);
		} catch (NumberFormatException e) {
			batchProgress = 10000;
		}
		log.info(".. Progress reporting  : " + batchProgress);

	}

	private static void progressLogging(String message) {
		NumberFormat pctFormat = NumberFormat.getPercentInstance();
		pctFormat.setMinimumFractionDigits(2);

		log.info(String.format(".. Progress : %s - %s",
				pctFormat.format(recordCount / totalRecords), message));
	}
}
