package org.iq80.twoLayerLog.util;

@SuppressWarnings("serial")
public class DecodeException extends CodingException {
	
	public DecodeException() {
		super();
	}
	
	public DecodeException(String message) {
		super(message);
	}
	
	public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public DecodeException(Throwable cause) {
        super(cause);
    }
}
