package org.iq80.twoLayerLog.util;

@SuppressWarnings("serial")
public class EncodeException extends CodingException {
	
	public EncodeException() {
		super();
	}
	
	public EncodeException(String message) {
		super(message);
	}
	
	public EncodeException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public EncodeException(Throwable cause) {
        super(cause);
    }
}
