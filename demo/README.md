# FifoQueue Demo

## Running the Demo
Import this demo project into Anypoint Studio. Run the project as a Mule application. Otherwise build this project and deploy in $MULE_HOME/apps.

## Purpose
This demo exposes 3 HTTP endpoints:

  - http://localhost:8081/put
  - http://localhost:8081/mark-error
  - http://localhost:8081/resolve-error

When the /put endpoint is hit from your browser, a message is put on a queue test.queue. This error will be read immediately by the take-all listener or the polling flow.

/mark-error will mark this queue as an error queue, hence if you invoke /put again, the message will be put on the queue but the listener and the poller will not be able to retrieve it.

/resolve-error will set the status of the queue back to normal. The poller will immediately start pulling the message from the queue.