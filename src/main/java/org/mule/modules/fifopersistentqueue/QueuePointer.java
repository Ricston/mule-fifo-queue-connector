/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifopersistentqueue;

public class QueuePointer {

	private String name;
	private long head;
	private long tail;
	private boolean status;

	public QueuePointer(String name) {
		this.name = name;
		head = 0L;
		tail = 0L;
		status = true;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getHead() {
		return head;
	}

	public void setHead(long head) {
		this.head = head;
	}

	public long getTail() {
		return tail;
	}

	public void setTail(long tail) {
		this.tail = tail;
	}

	public Long nextHead() {
		return head++;
	}

	public Long nextTail() {
		return tail++;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

}
