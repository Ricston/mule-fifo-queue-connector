/**
 * (c) 2006-2015 Ricston Ltd. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package com.ricston.connector.fifoqueue.exceptions;

public class OnlyOneAllListenerPermittedException extends Exception{

	private static final String MESSAGE = "Only one of each ALL listeners (peak-all/take-all) is allowed.";
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1012530947945960663L;
	
	public OnlyOneAllListenerPermittedException(){
		super(MESSAGE);
	}

}
