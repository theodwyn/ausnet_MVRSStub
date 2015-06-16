package spa;

import java.util.Properties;

public class RuntimeProperties extends Properties {

	private static final long serialVersionUID = 1L;
	private static RuntimeProperties instance = null;

	private RuntimeProperties() {
	}

	public static RuntimeProperties getInstance() {
		if (instance == null) {
			try {
				instance = new RuntimeProperties();
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		return instance;
	}
}