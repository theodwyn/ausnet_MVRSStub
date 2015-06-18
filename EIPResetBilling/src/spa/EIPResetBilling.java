package spa;

import org.apache.log4j.Logger;

import database.EIP_DAO;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;

public class EIPResetBilling {

	private final static String version = "V1.0. 2015-06-12";

	private static String propertiesFilename;
	private static String requestsFilename;

	// Change History
	// V1.0.1 2014-09-19 - Include optional insert into POLICY_EVENT
	// V1.0.0 2014-09-04 - Initial version

	private static Logger log = Logger.getLogger("EIPResetBilling");

	private static EIP_DAO dao;

	private static Request req = null;

	private static int recordCount = 0;
	private static int requestCount = 0;
	private static int batchLimit = 1000;

	static RuntimeProperties properties = RuntimeProperties.getInstance();

	public static void main(String[] args) throws InterruptedException, IOException {

		log.info("Start EIP Reset Billing");
		log.info(".. Version             : " + version);

		initialize(args);
		dao = new EIP_DAO();

		NumberFormat pctFormat = NumberFormat.getPercentInstance();
		pctFormat.setMinimumFractionDigits(2);

		ReadRequests requests = new ReadRequests(requestsFilename);
		log.info("request count: " + requests.getRequestsCount());

		req = requests.getNextRequest();

		// Main loop
		while (req != null) {
			recordCount++;

			if (req.getChannelNumber().equals("91")) {
				requestCount++;
				if (req.getInstallationType().equals("MRIM")) {
					dao.insertBILL_REQUEST(req);
				}
				
			}

			if (requestCount == batchLimit) {
				log.info(String.format(
						".. .. Batch : %s - %s",
						pctFormat.format((double) recordCount
								/ (double) requests.getRequestsCount()), req.toString()));
				dao.executeBatch();
				requestCount = 0;
			}

			req = requests.getNextRequest();
		}

		log.info(String.format(".. .. Batch : %s - %s",
				pctFormat.format((double) recordCount / (double) requests.getRequestsCount()),
				"Complete"));

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

		// Retrieve name of current host computer
		try {
			log.info(".. Host                : " + InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e2) {
			log.info(".. Host                : Unknown");
		}
	}

}
