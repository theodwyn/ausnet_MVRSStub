package spa;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import database.BILLING_REQUEST;
import database.DbUtil;
import database.EIP_DAO;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

public class EIPResetBilling {

	private final static String version = "V1.0. 2015-06-12";

	private static String propertiesFilename;
	private static String requestsFilename;
	private static String policyNameMask;

	// Change History
	// V1.0.1 2014-09-19 - Include optional insert into POLICY_EVENT
	// V1.0.0 2014-09-04 - Initial version

	private static Logger log = Logger.getLogger("EIPResetBilling");

	private static EIP_DAO dao;

	private static String env = null;
	private static Request req = null;

	private static int requestCount = 0;
	private static int requestTotal = 0;
	private static int insertCount = 0;
	private static int batchCount = 0;
	private static int batchLimit = 1000;

	static RuntimeProperties properties = RuntimeProperties.getInstance();

	public static void main(String[] args) throws InterruptedException, IOException {

		log.info("Start EIP Reset Billing");
		log.info(".. Version             : " + version);

		initialize(args);

		ReadRequests requests = new ReadRequests(requestsFilename);
		req = requests.getNextRequest();

		dao = new EIP_DAO();

		// Main loop
		while (req != null) {
			requestCount++;
	
			if (requestCount == batchLimit) {
				log.info(".. .. Batch : " + req.toString());
				requestTotal = requestTotal + requestCount;
				requestCount = 0;
			}

			req = requests.getNextRequest();
		}
		dao.closeConnection();

		log.info("End EIP Reset Billing");
	}

	// Load runtime properties
	private static void initialize(String[] args) {
		// parse the command line options
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(createOptions(), args);
		} catch (org.apache.commons.cli.ParseException e) {
			printCliHelp("Error in parsing arguments: " + e.getMessage());
		}

		// Validate that the requested environment is supported
		env = validateEnvironment(cmd);

		// Retrieve default runtime properties
		// - required 'env' argument to be passed and validated to locate
		// corresponding properties file
		propertiesFilename = "EIPResetBilling.properties";

		try {
			properties.load(new FileInputStream(propertiesFilename));
			// Confirm that the properties file matches the target environment
			if (!properties.getProperty("env").equals(env)) {
				log.error(".. Invalid target environment < " + env + " >");
				System.exit(-1);
			}
		} catch (Exception e) {
			log.error(".. Error accessing properties file < " + propertiesFilename + " >");
			System.exit(-1);
		}
		log.info(".. Target environment  : " + env);

		requestsFilename = properties.getProperty("requestsFilename");
		log.info(".. Requests filename   : " + requestsFilename);

		policyNameMask = properties.getProperty("policyNameMask");
		log.info(".. Policyname Regex    : " + policyNameMask);

		// Retrieve name of current host computer
		try {
			log.info(".. Host                : " + InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e2) {
			log.info(".. Host                : Unknown");
		}
	}

	// private method to build the list of options
	private static Options createOptions() {
		Options mOptions = new Options();

		OptionBuilder.withArgName("env");
		OptionBuilder.hasArg(true);
		OptionBuilder.withDescription("Target EIP environment");
		OptionBuilder.isRequired(true);
		mOptions.addOption(OptionBuilder.create("env"));

		return mOptions;
	}

	// private method to output the command line options help
	private static void printCliHelp(String message) {
		System.out.println(message);
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("Options", createOptions());
		System.exit(-1);
	}

	// private method to validate requested EIP test environment
	private static String validateEnvironment(CommandLine cmd) {

		final String[] validEnvironments = { "SIT", "UAT", "SVT" };
		String result = "";

		if (cmd.hasOption("env")) {
			result = cmd.getOptionValue("env");
			if (!ArrayUtils.contains(validEnvironments, result)) {
				log.error(".. Inavlid environment  < " + result + "  >");
				System.exit(-4);
			}
		}
		properties.setProperty("environment", result);
		return result;
	}

	/*
	 * private static String formatTimestamp(String seedTimestamp, int
	 * hourOffset) { Timestamp ts =
	 * DbUtil.formatStringToTimestamp(seedTimestamp); ts.setTime(ts.getTime() +
	 * hourOffset * 3600000); return DbUtil.formatTimestampToString(ts); }
	 */

}
