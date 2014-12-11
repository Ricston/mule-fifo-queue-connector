/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifoqueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.lifecycle.Stop;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIFO Queue Connector.
 * 
 * Using an object store to back single threaded queues.
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

	/**
	 * Logger
	 */
	private Logger logger = LoggerFactory.getLogger(getClass());

	// @Configurable
	// @Default(value="false")
	// private Boolean keepOnlyLastMessageOnError = false;

	/**
	 * A map used to store the queues' head and tail pointers, keyed by queue name
	 */
	private Map<String, QueuePointer> pointers = new HashMap<String, QueuePointer>();

	/**
	 * Map for inbound callbacks
	 */
	private Map<String, SourceCallback> peekCallbacks = new HashMap<String, SourceCallback>();
	private Map<String, SourceCallback> takeCallbacks = new HashMap<String, SourceCallback>();
	private SourceCallback peekAllCallback = null;
	private SourceCallback takeAllCallback = null;

	private static final String SEPARATOR = ":::";
	private static final String KEY = "%s" + SEPARATOR + "%d";
	private static final String STATUS = "status";
	private static final String STATUS_KEY = "%s" + SEPARATOR + STATUS;

	/**
	 * Find the QueuePointer within the pointers hash map. If not found, create one.
	 * 
	 * @param queue
	 *            The name of the queue
	 * @return The queue pointer
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
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
	 * Initialise the connector by opening the object store and loading the old messages.
	 * 
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@PostConstruct
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

				// tail contains the pointer to the next available (not current)
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
	 * Dispose the connector by closing the object store.
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
	 * Put a new message on the queue. This will automatically trigger callbacks.
	 * 
	 * Callback priority: fifo-queue:take-listener on specific queue, followed by fifo-queue:take-all-listener, followed by fifo-queue:peek-listener on specific
	 * queue, followed by fifo-queue:peek-all-listener. Only ONE callback will be called.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:put}
	 * 
	 * @param queue
	 *            The queue name
	 * @param content
	 *            Content to be processed
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 * @throws Exception
	 *             Any exception the source callback might throw
	 */
	@Processor
	public void put(String queue, @Payload Serializable content) throws ObjectStoreException, Exception {

		QueuePointer pointer = getPointer(queue);
		objectStore.store(formatQueueKey(queue, pointer.getTail()), content);
		pointer.nextTail();

		// check for callbacks
		SourceCallback callback = null;

		// If we have a take callback, take/remove the element off the queue before calling the callback.
		if ((callback = takeCallbacks.get(queue)) != null) {
			callback.process(take(pointer));
		} else if (takeAllCallback != null) {
			takeAllCallback.process(take(pointer));
		}
		// If we have a peek callback, use the content passed as parameter rather then peek(pointer). We cannot use peek(pointer) because if more than one
		// element is on the queue, peek will always return the first element.
		else if ((callback = peekCallbacks.get(queue)) != null) {
			callback.process(content);
		} else if (peekAllCallback != null) {
			peekAllCallback.process(content);
		}
	}

	/**
	 * Peek the head of the queue.
	 * 
	 * @param pointer
	 *            The queue
	 * @return The head of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected Serializable peek(QueuePointer pointer) throws ObjectStoreException {

		if (pointer.isStatus() && size(pointer) > 0) {
			return objectStore.retrieve(formatQueueKey(pointer.getName(), pointer.getHead()));
		}

		return null;
	}

	/**
	 * Peek the head of the queue.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:peek}
	 * 
	 * @param queue
	 *            The queue name
	 * @return The head of the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public Serializable peek(String queue) throws ObjectStoreException {

		QueuePointer pointer = getPointer(queue);
		return peek(pointer);
	}

	/**
	 * Peek a message from all queues with status OK.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:peek-all}
	 * 
	 * @return A list of messages, i.e. the head of every queue (with status OK)
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	@Processor
	public List<Serializable> peekAll() throws ObjectStoreException {

		List<Serializable> items = new ArrayList<Serializable>();

		for (Map.Entry<String, QueuePointer> pointer : pointers.entrySet()) {

			Serializable item = peek(pointer.getValue());
			if (item != null) {
				items.add(item);
			}
		}

		return items;
	}

	/**
	 * Take (remove) a message from the head of the queue.
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
	 * Take (remove) a message from the head of the queue.
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
	 * Take all items in a queue.
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
	 * Take all items in a queue.
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
	 * Take all items in all queues.
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
	 * Get the size of the queue.
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
	 * Get the size of the queue.
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
	 * Retrieve the current status of the queue.
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
	 * Set the queue status to error. This will stop the queue from returning any messages.
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
	 * This will mark the queue error free, hence it will start returning messages.
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
	 * Checks if there is a listener already registered for the queue.
	 * 
	 * @param queue
	 *            The queue name
	 * @throws OnlyOneListenerPermittedException
	 *             Thrown if a listener for the same queue is already registered
	 */
	protected void validateSingleListener(String queue) throws OnlyOneListenerPermittedException {
		if (peekCallbacks.containsKey(queue) || takeCallbacks.containsKey(queue)) {
			throw new OnlyOneListenerPermittedException(queue);
		}
	}

	/**
	 * Register peek callback for the queue (inbound endpoint). Once a message is received on the queue, peek will automatically be called and the item is
	 * passed to the callback. N.B. If fifo-queue:peek-listener is configured on a queue and 2 messages are received on the same queue, fifo-queue:peek-listener
	 * will be invoked twice with the correct message, however keep in mind that the messages will remain on the queue. Use case: connector is configured with a
	 * single receiver thread and at the end of the flow, fifo-queue:take is invoked to remove the message from the queue. Useful to keep the message on the
	 * queue until all processing is complete.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:peek-listener}
	 * 
	 * @param callback
	 *            The flow to be invoked
	 * @param queue
	 *            The queue name
	 * @throws OnlyOneListenerPermittedException
	 *             Thrown if a listener for the same queue is already registered
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 * @throws Exception
	 *             Any exception the source callback might throw
	 */
	@Source
	public void peekListener(SourceCallback callback, String queue) throws OnlyOneListenerPermittedException, ObjectStoreException, Exception {
		validateSingleListener(queue);
		peekCallbacks.put(queue, callback);

		// read messages that are already on the queue (on start up)
		QueuePointer pointer = getPointer(queue);
		List<Serializable> items = queueToList(pointer);
		for (Serializable item : items) {
			callback.process(item);
		}

	}

	/**
	 * Read all elements in a queue and return them as a list.
	 * 
	 * @param pointer
	 *            The queue Pointer
	 * @return A list containing all elements in the queue
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 */
	protected List<Serializable> queueToList(QueuePointer pointer) throws ObjectStoreException {
		List<Serializable> items = new ArrayList<Serializable>();

		if (pointer.isStatus() && size(pointer) > 0) {

			for (long position = pointer.getHead(); position < pointer.getTail(); position++) {
				items.add(objectStore.retrieve(formatQueueKey(pointer.getName(), position)));
			}
		}

		return items;
	}

	/**
	 * Register take callback for the queue (inbound endpoint). Once a message is received on the queue, take will automatically be called and the item is
	 * passed to the callback.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:take-listener}
	 * 
	 * @param callback
	 *            The flow to be invoked
	 * @param queue
	 *            The queue name
	 * @throws OnlyOneListenerPermittedException
	 *             Thrown if a listener for the same queue is already registered.
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 * @throws Exception
	 *             Any exception the source callback might throw
	 */
	@Source
	public void takeListener(SourceCallback callback, String queue) throws OnlyOneListenerPermittedException, ObjectStoreException, Exception {
		validateSingleListener(queue);
		takeCallbacks.put(queue, callback);

		// read messages that are already on the queue (on start up)
		QueuePointer pointer = getPointer(queue);

		Serializable item = null;
		while ((item = take(pointer)) != null) {
			callback.process(item);
		}
	}

	/**
	 * Register peek callback for all queues (inbound endpoint). Once a message is received on any queue (except the ones that have their own listeners), peek
	 * will automatically be called and the item is passed to the callback. N.B. If fifo-queue:peek-listener is configured on a queue and 2 messages are
	 * received on the same queue, fifo-queue:peek-listener will be invoked twice with the same (first) message. Reason: peek does not take the message off the
	 * queue. A use case of this would be when connector is configured with a single receiver thread and at the end of the flow, fifo-queue:take is invoked to
	 * remove the message from the queue.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:peek-all-listener}
	 * 
	 * @param callback
	 *            The flow to be invoked
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 * @throws Exception
	 *             Any exception the source callback might throw
	 */
	@Source
	public void peekAllListener(SourceCallback callback) throws ObjectStoreException, Exception {
		peekAllCallback = callback;

		// read messages that are already on the queue (on start up)
		for (Map.Entry<String, QueuePointer> pointerMapEntry : pointers.entrySet()) {

			QueuePointer pointer = pointerMapEntry.getValue();

			// skip operation if queue has its own listener
			if (!takeCallbacks.containsKey(pointer.getName()) || !peekCallbacks.containsKey(pointer.getName())) {
				List<Serializable> items = queueToList(pointer);
				for (Serializable item : items) {
					callback.process(item);
				}
			}
		}
	}

	/**
	 * Register take callback for all queues (inbound endpoint). Once a message is received on any queue (except the ones that have their own listeners), take
	 * will automatically be called and the item is passed to the callback.
	 * 
	 * {@sample.xml ../../../doc/fifo-queue-connector.xml.sample fifo-queue:take-all-listener}
	 * 
	 * @param callback
	 *            The flow to be invoked
	 * @throws ObjectStoreException
	 *             Any error the object store might throw
	 * @throws Exception
	 *             Any exception the source callback might throw
	 */
	@Source
	public void takeAllListener(SourceCallback callback) throws ObjectStoreException, Exception {
		takeAllCallback = callback;

		// read messages that are already on the queue (on start up)
		for (Map.Entry<String, QueuePointer> pointerMapEntry : pointers.entrySet()) {

			QueuePointer pointer = pointerMapEntry.getValue();

			// skip operation if queue has its own listener
			if (!takeCallbacks.containsKey(pointer.getName())) {
				Serializable item = null;
				while ((item = take(pointer)) != null) {
					callback.process(item);
				}
			}
		}

	}

	/**
	 * Set the queue status.
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
	 * Set the queue status.
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
	 * Given a queue and a position, create the key to be used within the object store to store the item.
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
	 * Get the object store.
	 * 
	 * @return The object store
	 */
	public ListableObjectStore<Serializable> getObjectStore() {
		return objectStore;
	}

	/**
	 * Set the object store.
	 * 
	 * @param objectStore
	 *            The object store
	 */
	public void setObjectStore(ListableObjectStore<Serializable> objectStore) {
		this.objectStore = objectStore;
	}

}