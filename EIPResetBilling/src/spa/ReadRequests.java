package spa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class ReadRequests {

	private FileInputStream fis = null;
	private InputStreamReader isr;
	private BufferedReader br;
	private String requestsFilename;

	private String requestRecord;
	private int requestsCount;

	public ReadRequests(String requestsFilename) {
		this.requestsFilename = requestsFilename;
		this.countRequests();

		try {
			fis = new FileInputStream(requestsFilename);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Request getNextRequest() throws IOException {
		Request req = null;

		requestRecord = br.readLine();
		if (requestRecord != null && !requestRecord.equals("")) {
			try {
				req = new Request(requestRecord);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return req;
	}

	private void countRequests() {
		try {
			LineNumberReader lnr = new LineNumberReader(new FileReader(new File(
					this.requestsFilename)));
			lnr.skip(Long.MAX_VALUE);

			this.setRequestsCount(lnr.getLineNumber());
			lnr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int getRequestsCount() {
		return requestsCount;
	}

	public void setRequestsCount(int requestsCount) {
		this.requestsCount = requestsCount;
	}

}
