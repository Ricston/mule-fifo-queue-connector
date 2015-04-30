# FifoQueue Anypoint Connector

FIFO Queue Connector. A connector that provides FIFO queues which internally uses the Mule Object Store to store the data. This connector was specifically
built to solve a problem with CloudHub where persistent queues are not guaranteed to be FIFO. This connector accepts any type of Object Store to store the
data, which includes persistent and in-memory object stores. To compliment another use case on CloudHub, this connector associates a status with each queue.
Queues can be marked with status OK or status ERROR. If the queue is marked with error, operations like peek and take will return null. Hence the messages
will keep piling in the queue until it is marked back to status OK. This is useful if you need to stop processing data until an issue is manually resolved.

# Mule supported versions

Mule 3.6.x

# Installation 
For beta connectors you can download the source code and build it with devkit to find it available on your local repository. Then you can add it to Studio…<TBD>

For released connectors you can download them from the update site in Studio. 
Open MuleStudio, go to Help → Install New Software and select MuleStudio Cloud Connectors Update Site where you’ll find all avaliable connectors.

#Usage
For information about usage our documentation at https://github.com/Ricston/mule-fifo-queue-connector/wiki.

# Reporting Issues

We use GitHub:Issues for tracking issues with this connector. You can report new issues at this link https://github.com/Ricston/mule-fifo-queue-connector/issues.
=======
