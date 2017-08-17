This directory contains a RabbitMQ message producer written in Java,
suitable for sending messages to start OODT workflows.

Usage:

o Compile the source code into the client jar:

mvn package

o Include the jar file and all its dependencies inside your project:

target/rabbitmq-clients-0.1.jar

target/lib/amqp-client-4.0.2.jar
target/lib/json-20160212.jar
target/lib/slf4j-api-1.7.21.jar
target/lib/slf4j-simple-1.7.22.jar

o Define an enviromental variable that contains the connection parameters to the RabbitMQ server, for example:

export RABBITMQ_USER_URL=amqp://USERNAME:PASSWORD@localhost/%2f

o Within your Java code, when it's time to start a workflow, do the following
(see example in oodt.docker.rabbitmq.clients.RabbitmqProducer.main()) :
	- instantiate a RabbitmqProducer object
	- package all necessary metadata into a Map object and invoke the method: sendEvent(workflowName, metadata)
	- close the connection to the Rabbitmq server
