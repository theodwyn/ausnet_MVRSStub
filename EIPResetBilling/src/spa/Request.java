package spa;

public class Request {
	private String SDP;
	private String SDPRef;
	private String MeterId;
	private String MeterRef;
	private String MeterProgram;
	private String ChannelRef;
	private String ChannelNumber;
	private String InstallationType;

	public Request(String requestRecord) {
		// 6305752770-1 ,4219388 ,1-2AZHW5 ,4102 ,1-NMQW ,91,MRIM
		String[] requestParts = requestRecord.split(",");

		this.SDP = requestParts[0].trim();
		this.SDPRef = requestParts[1].trim();
		this.MeterId = requestParts[2].trim();
		this.MeterRef = requestParts[3].trim();
		this.MeterProgram = requestParts[4].trim();
		this.ChannelRef = requestParts[5].trim();
		this.ChannelNumber = requestParts[6].trim();
		this.InstallationType = requestParts[7].trim();
	}

	public String getSDP() {
		return SDP;
	}

	public void setSDP(String sDP) {
		SDP = sDP;
	}

	public String getSDPRef() {
		return SDPRef;
	}

	public void setSDPRef(String sDPRef) {
		SDPRef = sDPRef;
	}

	public String getMeterId() {
		return MeterId;
	}

	public void setMeterId(String meterId) {
		MeterId = meterId;
	}

	public String getMeterRef() {
		return MeterRef;
	}

	public void setMeterRef(String meterRef) {
		MeterRef = meterRef;
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
		return "Request [SDP=" + SDP + ", SDPRef=" + SDPRef + ", MeterId=" + MeterId
				+ ", MeterRef=" + MeterRef + ", MeterProgram=" + MeterProgram + ", ChannelRef="
				+ ChannelRef + ", ChannelNumber=" + ChannelNumber + ", InstallationType="
				+ InstallationType + "]";
	}

}
