/**
 * (c) 2006-2015 Ricston Ltd. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifoqueue.exceptions;

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
