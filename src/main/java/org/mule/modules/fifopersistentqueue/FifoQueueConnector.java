/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifopersistentqueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.lifecycle.Start;
import org.mule.api.annotations.lifecycle.Stop;
import org.mule.api.annotations.param.Payload;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIFO Queue Connector
 * 
 * Using an object store to back a single threaded queue
 * 
 * @author MuleSoft, Inc.
 */
@Connector(name = "fifo-queue", schemaVersion = "1.0", friendlyName = "FifoQueue")
public class FifoQueueConnector {
	/**
	 * The object store used to store the queue messages
	 */
	@Configurable
	private ListableObjectStore<Serializable> objectStore;
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	// @Configurable
	// @Default(value="false")
	// private Boolean keepOnlyLastMessageOnError;

	/**
	 * A map used to store the queues' head and tail pointers, keyed by queue name
	 */
	private Map<String, QueuePointer> pointers = new HashMap<String, QueuePointer>();

	private static final String SEPARATOR = ":::";
	private static final String KEY = "%s" + SEPARATOR + "%d";
	private static final String STATUS = "status";
	private static final String STATUS_KEY = "%s" + SEPARATOR + STATUS;

	protected QueuePointer getPointer(String queue) throws ObjectStoreException {

		QueuePointer pointer = pointers.get(queue);

		// if the pointer does not exist, its the first time we are
		// encountering this queue, so we need to create it
		if (pointer == null) {
			pointer = new QueuePointer(queue);
			pointers.put(queue, pointer);
			queueStatus(pointer, true);
		}

		return pointer;
	}

	/**
	 * Initialise the connector by opening the object store and loading the old messages
	 * 
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Start
	public void initialiseConnector() throws ObjectStoreException {

		objectStore.open();

		List<Serializable> keys = objectStore.allKeys();

		for (Serializable key : keys) {
			String stringKey = (String) key;
			String[] separatedKey = stringKey.split(SEPARATOR);

			// queue name is the first part of the key
			String queue = separatedKey[0];
			QueuePointer pointer = getPointer(queue);

			// if the second part of the key is the text "status", then
			// this field marks the status of the queue
			if (separatedKey[1].equals(STATUS)) {
				pointer.setStatus((Boolean) objectStore.retrieve(stringKey));
			}
			// otherwise, the second part of the key is the sequence number
			// of the entry in the object store
			else {
				Long sequenceNumber = Long.parseLong(separatedKey[1]);

				if (pointer.getHead() > sequenceNumber) {
					pointer.setHead(sequenceNumber);
				}

				//tail contains the pointer to the next available (not current)
				sequenceNumber++;
				if (pointer.getTail() < sequenceNumber) {
					pointer.setTail(sequenceNumber);
				}
			}
		}
		
		logger.info("Initialisation complete, status restored");
	}

	/**
	 * 
	 * Dispose the connector by closing the object store
	 * 
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Stop
	public void disposeConnector() throws ObjectStoreException {
		pointers.clear();
		objectStore.close();
	}

	/**
	 * Put
	 * 
	 * Put a new message on the queue
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:put}
	 * 
	 * @param queue
	 *            The queue name
	 * @param content
	 *            Content to be processed
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public void put(String queue, @Payload Serializable content) throws ObjectStoreException {

		QueuePointer pointer = getPointer(queue);
		objectStore.store(formatQueueKey(queue, pointer.getTail()), content);
		pointer.nextTail();
	}

	/**
	 * Peak the head of the queue
	 * 
	 * @param pointer
	 *            The queue
	 * @return The head of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected Serializable peak(QueuePointer pointer) throws ObjectStoreException {

		if (pointer.isStatus() && size(pointer) > 0) {
			return objectStore.retrieve(formatQueueKey(pointer.getName(), pointer.getHead()));
		}

		return null;
	}

	/**
	 * Peak
	 * 
	 * Peak the head of the queue
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:peak}
	 * 
	 * @param queue
	 *            The queue name
	 * @return The head of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public Serializable peak(String queue) throws ObjectStoreException {

		QueuePointer pointer = getPointer(queue);
		return peak(pointer);
	}

	/**
	 * peakAll
	 * 
	 * Peak a message from all queues with status OK
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:peak-all}
	 * 
	 * @return A list of messages, i.e. the head of every queue (with status OK)
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public List<Serializable> peakAll() throws ObjectStoreException {

		List<Serializable> items = new ArrayList<Serializable>();

		for (Map.Entry<String, QueuePointer> pointer : pointers.entrySet()) {

			Serializable item = peak(pointer.getValue());
			if (item != null) {
				items.add(item);
			}
		}

		return items;
	}

	/**
	 * Take (remove) a message from the head of the queue
	 * 
	 * @param pointer
	 *            The queue
	 * @return The head of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected Serializable take(QueuePointer pointer) throws ObjectStoreException {

		if (pointer.isStatus() && size(pointer) > 0) {
			Serializable item = objectStore.remove(formatQueueKey(pointer.getName(), pointer.getHead()));
			pointer.nextHead();
			return item;
		}

		return null;
	}

	/**
	 * Take
	 * 
	 * Take (remove) a message from the head of the queue
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:take}
	 * 
	 * @param queue
	 *            The queue name
	 * @return The head of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public Serializable take(String queue) throws ObjectStoreException {

		QueuePointer pointer = getPointer(queue);
		return take(pointer);
	}

	/**
	 * takeAll
	 * 
	 * Take (remove) a message from all queues with status OK
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:take-all}
	 * 
	 * @return A list of messages, i.e. the head of every queue (with status OK)
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public List<Serializable> takeAll() throws ObjectStoreException {

		List<Serializable> items = new ArrayList<Serializable>();

		for (Map.Entry<String, QueuePointer> pointer : pointers.entrySet()) {

			Serializable item = take(pointer.getValue());
			if (item != null) {
				items.add(item);
			}
		}

		return items;
	}

	/**
	 * Take all items in a queue
	 * 
	 * @param pointer
	 *            The queue pointer
	 * @return A list of all items in the queue, in FIFO order
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected List<Serializable> drain(QueuePointer pointer) throws ObjectStoreException {
		List<Serializable> items = new ArrayList<Serializable>();

		Serializable item;
		while ((item = take(pointer)) != null) {
			items.add(item);
		}

		return items;
	}

	/**
	 * drain
	 * 
	 * Take all items in a queue
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:drain}
	 * 
	 * @param queue
	 *            The queue name
	 * @return A list of all items in the queue, in FIFO order
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public List<Serializable> drain(String queue) throws ObjectStoreException {
		QueuePointer pointer = getPointer(queue);
		return drain(pointer);
	}

	/**
	 * 
	 * drainAll
	 * 
	 * Take all items in all queues
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:drain-all}
	 * 
	 * @return A map of lists of all items in all queues, in FIFO order by queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public Map<String, List<Serializable>> drainAll() throws ObjectStoreException {

		Map<String, List<Serializable>> items = new HashMap<String, List<Serializable>>();

		for (Map.Entry<String, QueuePointer> pointer : pointers.entrySet()) {

			List<Serializable> queueItems = drain(pointer.getValue());
			items.put(pointer.getKey(), queueItems);
		}

		return items;
	}

	/**
	 * Get the size of the queue
	 * 
	 * @param pointer
	 *            The queue pointer
	 * @return The size of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	public long size(QueuePointer pointer) throws ObjectStoreException {
		return pointer.getTail() - pointer.getHead();
	}

	/**
	 * Size
	 * 
	 * Get the size of the queue
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:size}
	 * 
	 * @param queue
	 *            The queue name
	 * @return The size of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public long size(String queue) throws ObjectStoreException {
		QueuePointer pointer = getPointer(queue);
		return pointer.getTail() - pointer.getHead();
	}

	/**
	 * Status
	 * 
	 * Retrieve the current status of the queue
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:status}
	 * 
	 * @param queue
	 *            The queue name
	 * @return The status of the queue, true for OK, false for error
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public boolean status(String queue) throws ObjectStoreException {
		QueuePointer pointer = getPointer(queue);
		return pointer.isStatus();
	}

	/**
	 * markError
	 * 
	 * Set the queue status to error. This will stop the queue from returning any messages
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:mark-error}
	 * 
	 * @param queue
	 *            The queue name
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public void markError(String queue) throws ObjectStoreException {
		queueStatus(queue, false);
	}

	/**
	 * resolveError
	 * 
	 * This will mark the queue error free, hence it will start returning messages
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:resolve-error}
	 * 
	 * @param queue
	 *            The queue name
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public void resolveError(String queue) throws ObjectStoreException {
		queueStatus(queue, true);
	}

	/**
	 * Set the queue status
	 * 
	 * @param queue
	 *            The queue name
	 * @param status
	 *            The status of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected void queueStatus(String queue, boolean status) throws ObjectStoreException {
		QueuePointer pointer = getPointer(queue);
		queueStatus(pointer, status);
	}

	/**
	 * 
	 * Set the queue status
	 * 
	 * @param pointer
	 *            The queue pointer
	 * @param status
	 *            The status of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected void queueStatus(QueuePointer pointer, boolean status) throws ObjectStoreException {
		pointer.setStatus(status);

		String key = String.format(STATUS_KEY, pointer.getName());

		if (objectStore.contains(key)) {
			objectStore.remove(key);
		}

		objectStore.store(key, status);
	}

	/**
	 * 
	 * @param queue
	 *            The queue name
	 * @param position
	 *            The position of the entry
	 * @return The key to use in the object store to store/retrieve this queue element
	 */
	protected String formatQueueKey(String queue, long position) {
		return String.format(KEY, queue, position);
	}

	/**
	 * Get the object store
	 * 
	 * @return The object store
	 */
	public ListableObjectStore<Serializable> getObjectStore() {
		return objectStore;
	}

	/**
	 * Set the object store
	 * 
	 * @param objectStore
	 *            The object store
	 */
	public void setObjectStore(ListableObjectStore<Serializable> objectStore) {
		this.objectStore = objectStore;
	}

}