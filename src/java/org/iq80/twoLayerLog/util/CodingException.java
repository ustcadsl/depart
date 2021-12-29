package org.iq80.twoLayerLog.util;

@SuppressWarnings("serial")
public class CodingException extends RuntimeException {
	
	public CodingException() {
		super();
	}
	
	public CodingException(String message) {
		super(message);
	}
	
	public CodingException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public CodingException(Throwable cause) {
        super(cause);
    }
}
