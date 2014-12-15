/**
 * (c) 2006-2015 Ricston Ltd. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

/**
 * (c) 2003-2014 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.fifoqueue;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.client.MuleClient;
import org.mule.construct.Flow;
import org.mule.modules.tests.ConnectorTestCase;

public class FifoQueueListenerConnectorTest extends ConnectorTestCase{
	
	//some functions will be repeated
	private long loop = 5;
	
	//number of queues in test config (to be used by takeAll and peekAll)
	private int numberOfQueues = 4;
	private int numberOfQueuesWithTakeListener = 1;
	private int numberOfQueuesWithPeekListener = 1;
	
	private String payload = "Another string ";
	private MuleClient client;
	private static final int DEFAULT_TIMEOUT = 10000;
	
	@Override
    protected String getConfigResources() {
        return "fifo-queue-listener-config.xml";
    }
	
	@Before
	public void initialise() throws Exception{
		client = muleContext.getClient();
		
		logger.info("Inserting messages");
    	
    	//insert in queues
    	for (int i=0; i<loop; i++){
    		runFlow("putFlow", payload + i);
    	}
	}
	
	@Test
	public void testPeekListener() throws MuleException{
		MuleMessage result;
		
		for (int i=0; i<loop; i++){
			result = client.request("vm://testcase.peek", DEFAULT_TIMEOUT);
			Assert.assertEquals(payload + i, result.getPayload());
		}
	}
	
	@Test
	public void testTakeListener() throws MuleException{
		MuleMessage result;
		
		for (int i=0; i<loop; i++){
			result = client.request("vm://testcase.take", DEFAULT_TIMEOUT);
			Assert.assertEquals(payload + i, result.getPayload());
		}
	}
	
	@Test
	public void testPeekAllListener() throws MuleException{
		Flow flow = (Flow) muleContext.getRegistry().lookupFlowConstruct("peekAllListenerFlow");
		flow.start();
		
		//should get 2 messages (2 queues), and 5 messages per queue
		MuleMessage result;
		
		for (int j=0; j<(numberOfQueues - numberOfQueuesWithTakeListener - numberOfQueuesWithPeekListener); j++){
			for (int i=0; i<loop; i++){
				result = client.request("vm://testcase.peekAll", DEFAULT_TIMEOUT);
				Assert.assertEquals(payload + i, result.getPayload());
			}
		}
		
		//check there are no other messages
		result = client.request("vm://testcase.peekAll", 100);
		Assert.assertNull(result);
	}
	
	@Test
	public void testTakeAllListener() throws MuleException{
		Flow flow = (Flow) muleContext.getRegistry().lookupFlowConstruct("takeAllListenerFlow");
		flow.start();
		
		//should get 3 messages (3 queues), and 5 messages per queue
		MuleMessage result;
		
		for (int j=0; j<(numberOfQueues - numberOfQueuesWithTakeListener); j++){
			for (int i=0; i<loop; i++){
				result = client.request("vm://testcase.takeAll", DEFAULT_TIMEOUT);
				Assert.assertEquals(payload + i, result.getPayload());
			}
		}
		
		//check there are no other messages
		result = client.request("vm://testcase.takeAll", 100);
		Assert.assertNull(result);
	}
}
