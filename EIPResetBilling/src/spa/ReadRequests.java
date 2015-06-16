package spa;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReadRequests {

	private FileInputStream fis = null;
	private InputStreamReader isr;
	private BufferedReader br;

	private int requestCount = 0;
	private String requestRecord;

	public ReadRequests(String requestsFilename) {

		try {
			fis = new FileInputStream(requestsFilename);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			requestRecord = br.readLine();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public Request getNextRequest() throws IOException {
		Request req = null;

		requestRecord = br.readLine();
		while (requestRecord != null) {
			requestCount++;
			req = new Request(requestCount, requestRecord);
			requestRecord = br.readLine();
		}
		return req;
	}
}
