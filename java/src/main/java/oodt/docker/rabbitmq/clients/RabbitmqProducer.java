package oodt.docker.rabbitmq.clients;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP;

import java.util.Map;
import java.util.HashMap;
import org.json.JSONObject;
import java.util.UUID;

/**
  Rabbitmq message producer for OODT workflows.
*/
public class RabbitmqProducer {

  // fixed parameters
  private final static String EXCHANGE_NAME = "oodt-exchange";
  private final static String EXCHANGE_TYPE = "direct";

  // instance properties
  private String producerId = UUID.randomUUID().toString();
  private Connection connection = null;
  private Channel channel = null;

  /**
    Initializes connection to RabbitMQ server from method arguments.
  */
  public RabbitmqProducer(String hostName, int portNumber, String userName, String password) throws Exception {

    // create connection to RMQ broker
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(hostName);
    factory.setPort(portNumber);
    factory.setUsername(userName);
    factory.setPassword(password);
    this.connection = factory.newConnection();

    // create channel
    this.channel = connection.createChannel();
   
    // create exchange
    boolean durable = true;
    channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, durable);

  }

 /**
    Initializes connection to RabbitMQ server from environmental variable RABBITMQ_USER_URL
    Example: export RABBITMQ_USER_URL=amqp://USERNAME:PASSWORD@localhost/%2f
  */
  public RabbitmqProducer() throws Exception {

    String uri = System.getenv("RABBITMQ_USER_URL");
    
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUri(uri);
    this.connection = factory.newConnection();

    // create channel
    this.channel = connection.createChannel();

    // create exchange
    boolean durable = true;
    channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, durable);

  }

  public void sendEvent(String workflowEvent, Map<String, String> metadata) throws Exception {

    // create queue for this workflow, if not existing already
    String queueName = workflowEvent;
    String routingKey = workflowEvent;
    boolean durable = true;
    boolean exclusive = false;
    boolean autoDelete = false;
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);

    // bind queue to existing exchange
    channel.queueBind(queueName, EXCHANGE_NAME, routingKey);

    // message headers (same content as message body)
    Map<String, Object> headers = new HashMap<String, Object>();
    headers.putAll( metadata );

    // publish message
    JSONObject json = new JSONObject(metadata);
    String message = json.toString();
    channel.basicPublish(EXCHANGE_NAME, 
                         routingKey,
                         new AMQP.BasicProperties.Builder()
                                 .contentType("application/json")
                                 .deliveryMode(2)
                                 .headers( headers )
                                 .appId(this.producerId)
                                 .build(),
                         message.getBytes("UTF-8") );

    System.out.println(" [x] Sent '" + message + "'");

  }

  /**
    Closes connection and channel
  */
  public void close() throws Exception {

    this.channel.close();
    this.connection.close();

  }


  /**
    Example main program to invoke the RabbitmqProducer.
    Usage: java -Djava.ext.dirs=./lib oodt.docker.rabbitmq.clients.RabbitmqProducer RabbitmqProducer <workflow_name> <number_of_runs>
    Example: java -Djava.ext.dirs=./lib oodt.docker.rabbitmq.clients.RabbitmqProducer RabbitmqProducer test-workflow 10
  */
  public static void main(String[] argv) throws Exception {

    if (argv.length!=2) {
        System.out.println("Usage: java -Djava.ext.dirs=./lib oodt.docker.rabbitmq.clients.RabbitmqProducer RabbitmqProducer <workflow_name> <number_of_runs>");
        System.exit(-1);
    }
    String workflowName = argv[0];
    int numberofRuns = Integer.parseInt(argv[1]);

    // open connection to RabbitMQ server
    RabbitmqProducer rmqp = new RabbitmqProducer();

    // send messages
    for (int i=1; i<=numberofRuns; i++) {
       Map<String,String> metadata = new HashMap<String,String>() {};
       metadata.put("Dataset","abc");
       metadata.put("Project","123");
       metadata.put("Run",""+i);
       rmqp.sendEvent(workflowName, metadata);
    }

    // close connection to RabbitMQ server
    rmqp.close();

  }

}
