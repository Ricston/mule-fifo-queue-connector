package org.mule.modules.fifoqueue;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.client.MuleClient;
import org.mule.modules.tests.ConnectorTestCase;

public class FifoQueueListenerConnectorTest extends ConnectorTestCase{
	
	private String payload = "Another string ";
	private MuleClient client;
	private static final int DEFAULT_TIMEOUT = 10000;
	
	@Before
	public void initialise() throws Exception{
		client = muleContext.getClient();
		runFlow("putFlow", payload);
	}
	
	@Override
    protected String getConfigResources() {
        return "fifo-queue-listener-config.xml";
    }
	
	@Test
	public void testPeakListener() throws MuleException{
		MuleMessage result = client.request("vm://testcase.peak", DEFAULT_TIMEOUT);
		Assert.assertEquals(payload, result.getPayload());
	}
	
	@Test
	public void testTakeListener() throws MuleException{
		MuleMessage result = client.request("vm://testcase.take", DEFAULT_TIMEOUT);
		Assert.assertEquals(payload, result.getPayload());
	}
}
