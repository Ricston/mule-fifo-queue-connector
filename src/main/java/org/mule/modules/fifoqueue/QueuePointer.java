/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifoqueue;

import java.util.concurrent.atomic.AtomicLong;

public class QueuePointer {

	private String name;
	private AtomicLong head;
	private AtomicLong tail;
	private boolean status;

	public QueuePointer(String name) {
		this.name = name;
		head = new AtomicLong(0);
		tail = new AtomicLong(0);
		status = true;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getHead() {
		return head.get();
	}

	public void setHead(long head) {
		this.head.set(head);
	}

	public long getTail() {
		return tail.get();
	}

	public void setTail(long tail) {
		this.tail.set(tail);
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}
	
	public long fetchAndAddHead(){
		return this.head.getAndAdd(1);
	}
	
	public long fetchAndAddTail(){
		return this.tail.getAndAdd(1);
	}

}
