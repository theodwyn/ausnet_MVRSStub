package spa;

import java.util.Arrays;

public class Request {

	private String RequestRecord;
	String[] requestParts;

	private String SDP;
	private String MeterId;
	private String MeterProgram;
	private String ChannelRef;
	private String ChannelNumber;
	private String InstallationType;

	public Request(int requestCount, String requestRecord) {
		this.requestParts = this.RequestRecord.split(",");

		this.SDP = requestParts[0].trim();
		this.MeterId = requestParts[1].trim();
		this.MeterProgram = requestParts[2].trim();
		this.ChannelRef = requestParts[3].trim();
		this.ChannelNumber = requestParts[4].trim();
		this.InstallationType = requestParts[5].trim();
	}

	public String getRequestRecord() {
		return RequestRecord;
	}

	public void setRequestRecord(String requestRecord) {
		RequestRecord = requestRecord;
	}

	public String getSDP() {
		return SDP;
	}

	public void setSDP(String sDP) {
		SDP = sDP;
	}

	public String getMeterId() {
		return MeterId;
	}

	public void setMeterId(String meterId) {
		MeterId = meterId;
	}

	public String getMeterProgram() {
		return MeterProgram;
	}

	public void setMeterProgram(String meterProgram) {
		MeterProgram = meterProgram;
	}

	public String getChannelRef() {
		return ChannelRef;
	}

	public void setChannelRef(String channelRef) {
		ChannelRef = channelRef;
	}

	public String getChannelNumber() {
		return ChannelNumber;
	}

	public void setChannelNumber(String channelNumber) {
		ChannelNumber = channelNumber;
	}

	public String getInstallationType() {
		return InstallationType;
	}

	public void setInstallationType(String installationType) {
		InstallationType = installationType;
	}

	@Override
	public String toString() {
		return "Request [RequestRecord=" + RequestRecord + ", requestParts="
				+ Arrays.toString(requestParts) + ", SDP=" + SDP + ", MeterId=" + MeterId
				+ ", MeterProgram=" + MeterProgram + ", ChannelRef=" + ChannelRef
				+ ", ChannelNumber=" + ChannelNumber + ", InstallationType=" + InstallationType
				+ "]";
	}

}
