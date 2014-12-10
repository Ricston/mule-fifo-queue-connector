package org.mule.modules.fifoqueue;

public class OnlyOneListenerPermittedException extends Exception{

	private static final String MESSAGE = "Only one listener (peak/take) is allowed for each queue. Queue ** %s ** has more then one listener";
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1012530947945960663L;
	
	public OnlyOneListenerPermittedException(String queue){
		super(String.format(MESSAGE, queue));
	}

}
