/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifoqueue;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mule.api.MuleEvent;
import org.mule.api.registry.MuleRegistry;
import org.mule.modules.fifoqueue.FifoQueueConnector;
import org.mule.modules.tests.ConnectorTestCase;
import org.mule.transport.NullPayload;

public class FifoQueueConnectorTest extends ConnectorTestCase {
	
	//some functions will be repeated
	private long loop = 5;
	
	//number of queues in test config (to be used by takeAll and peakAll)
	private int numberOfQueues = 5;
	private String payload = "Another string ";
    
    @Override
    protected String getConfigResources() {
        return "fifo-queue-config.xml";
    }
    
    @Before
    public void initialise() throws Exception{
    	
    	logger.info("Inserting messages");
    	
    	//insert in queue
    	for (int i=0; i<loop; i++){
    		runFlow("putFlow", payload + i);
    	}
    }
    
    @After
    public void tearDown() throws Exception{
    	logger.info("Removing all messages");
    	runFlow("drainAllFlow");
    }
    
    @Test
    public void testPutAndSize() throws Exception{
    	//check we have the correct number of elements in queue
    	MuleEvent result = runFlow("sizeFlow");
    	Assert.assertEquals(loop, result.getMessage().getPayload());
    }
    
    @Test
    public void testPeek() throws Exception {
    	
    	MuleEvent result;
    	
    	logger.info("Peeking messages");
    	
    	//peak the queue
    	for (int i=0; i<loop; i++){
    		result = runFlow("peakFlow");
    		Assert.assertEquals(payload + "0", result.getMessage().getPayload());
    	}
    	
    	logger.info("Asserting size");
    	
    	//check we have the correct number of elements in queue (nothing should have been removed)
    	result = runFlow("sizeFlow");
    	Assert.assertEquals(loop, result.getMessage().getPayload());
    }

    @Test
    public void testTake() throws Exception {
    	
    	MuleEvent result;
    	
    	logger.info("Taking messages");
    	
    	//take elements from the queue
    	for (int i=0; i<loop; i++){
    		result = runFlow("takeFlow");
    		Assert.assertEquals(payload + i, result.getMessage().getPayload());
    	}
    	
    	logger.info("Asserting size");
    	
    	//check we have the correct number of elements in queue (all elements should have been removed)
    	result = runFlow("sizeFlow");
    	Assert.assertEquals(0L, result.getMessage().getPayload());
    }
    
    @Test
    public void testMarkError() throws Exception{
    	
    	//mark the queue with status error
    	runFlow("markErrorFlow");
    	
    	//take a message from the queue, should return null since queue is marked with error
    	MuleEvent result = runFlow("takeFlow");
    	Assert.assertEquals(NullPayload.getInstance(), result.getMessage().getPayload());
    	
    }
    
    @Test
    public void testResolveError() throws Exception{
    	
    	//mark the queue with error
    	testMarkError();
    	
    	//resolve the error
    	runFlow("resolveErrorFlow");
    	
    	//take a message from the queue, should return the original message since error was fixed
    	MuleEvent result = runFlow("takeFlow");
    	Assert.assertEquals(payload + 0, result.getMessage().getPayload());
    }
    
    @Test
    public void testStatus() throws Exception{
    	//mark the queue with error
    	testMarkError();
    	
    	MuleEvent result = runFlow("statusFlow");
    	Assert.assertEquals(false, result.getMessage().getPayload());
    	
    	//resolve error
    	testResolveError();
    	result = runFlow("statusFlow");
    	Assert.assertEquals(true, result.getMessage().getPayload());
    }
    
    @Test
    public void testPeakAll() throws Exception{
    	MuleEvent result;
    	
    	//peak elements from the queue
    	for (int i=0; i<loop; i++){
	    	result = runFlow("peakAllFlow");
	    	Assert.assertTrue(result.getMessage().getPayload() instanceof List);
	    	@SuppressWarnings("unchecked")
			List<Serializable> items = (List<Serializable>) result.getMessage().getPayload();
	    	Assert.assertEquals(numberOfQueues, items.size());
	    	
	    	for(int j=0; j<numberOfQueues; j++){
	    		Assert.assertEquals(payload + 0, items.get(j));
	    	}
    	}
    	
    	//check we have the correct number of elements in queue (nothing should have been removed)
    	result = runFlow("sizeFlow");
    	Assert.assertEquals(loop, result.getMessage().getPayload());
    }
    
    @Test
    public void testTakeAll() throws Exception{
    	
    	MuleEvent result;
    	
    	//take elements from the queue
    	for (int i=0; i<loop; i++){
	    	result= runFlow("takeAllFlow");
	    	Assert.assertTrue(result.getMessage().getPayload() instanceof List);
	    	@SuppressWarnings("unchecked")
			List<Serializable> items = (List<Serializable>) result.getMessage().getPayload();
	    	Assert.assertEquals(numberOfQueues, items.size());
	    	
	    	for(int j=0; j<numberOfQueues; j++){
	    		Assert.assertEquals(payload + i, items.get(j));
	    	}
	    	
    	}
    	//check we have the correct number of elements in queue (all elements should have been removed)
    	result = runFlow("sizeFlow");
    	Assert.assertEquals(0L, result.getMessage().getPayload());
    }
    
    @Test
    public void testDrain() throws Exception {
    	
    	logger.info("Draining queue");

    	//take elements from the queue
    	MuleEvent result = runFlow("drainFlow");
    	Assert.assertTrue(result.getMessage().getPayload() instanceof List);
    	@SuppressWarnings("unchecked")
		List<Serializable> items = (List<Serializable>) result.getMessage().getPayload();
    	Assert.assertEquals(loop, items.size());
    	
    	for (int i=0; i<loop; i++){
    		Assert.assertEquals(payload + i, items.get(i));
    	}
    	
    	logger.info("Asserting size");
    	
    	//check we have the correct number of elements in queue (all elements should have been removed)
    	result = runFlow("sizeFlow");
    	Assert.assertEquals(0L, result.getMessage().getPayload());
    }
    
    @Test
    public void testDrainAll() throws Exception {
    	
    	logger.info("Draining all queues");

    	//take elements from the queue
    	MuleEvent result = runFlow("drainAllFlow");
    	Assert.assertTrue(result.getMessage().getPayload() instanceof Map);
    	@SuppressWarnings("unchecked")
		Map<String, List<Serializable>> items = (Map<String, List<Serializable>>) result.getMessage().getPayload();
    	Assert.assertEquals(numberOfQueues, items.size());
    	
    	for(List<Serializable> queueItems : items.values()){
    		Assert.assertEquals(loop, queueItems.size());
	    	for (int i=0; i<loop; i++){
	    		Assert.assertEquals(payload + i, queueItems.get(i));
	    	}
    	}
    	
    	logger.info("Asserting size");
    	
    	//check we have the correct number of elements in queue (all elements should have been removed)
    	result = runFlow("sizeFlow");
    	Assert.assertEquals(0L, result.getMessage().getPayload());
    }
    
    @Test
    public void initialiseConnector() throws Exception{
    	
    	MuleRegistry registry = muleContext.getRegistry();
    	
    	//get the connector
    	FifoQueueConnector connector = (FifoQueueConnector) registry.lookupObject("fifoQueueConnector");
    	
    	//destroy the connector
    	connector.disposeConnector();
    	
    	//recreate the connector
    	connector.initialiseConnector();
    	
    	//since status should be exactly the same, lets try to drain all
    	testDrainAll();
    	
    }
}
