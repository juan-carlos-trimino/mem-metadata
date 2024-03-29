/***
It records details and metadata about each video.

* The app contains a single RabbitMQ server instance; the RabbitMQ server contains multiple queues
  with different names.
* Each microservice has its own private database; the databases are hosted on a shared server.
***/
const express = require('express');
const mongodb = require('mongodb');
const mongodbClient = require('mongodb').MongoClient;
const amqp = require('amqplib');
const bodyParser = require('body-parser');
const winston = require('winston');
const { randomUUID } = require('crypto');

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_NAME = process.env.SVC_NAME;
const APP_NAME_VER = process.env.APP_NAME_VER;
const DB_NAME = process.env.DB_NAME;
const SVC_DNS_DB = process.env.SVC_DNS_DB;
const SVC_DNS_RABBITMQ = process.env.SVC_DNS_RABBITMQ;
const PORT = process.env.PORT && parseInt(process.env.PORT) || 3000;
const MAX_RETRIES = process.env.MAX_RETRIES && parseInt(process.env.MAX_RETRIES) || 10;
let READINESS_PROBE = false;

/***
Resume Operation
----------------
The resume operation strategy intercepts unexpected errors and responds by allowing the process to
continue.
***/
process.on('uncaughtException',
err => {
  logger.error('Uncaught exception.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
})

/***
Abort and Restart
-----------------
***/
// process.on("uncaughtException",
// err => {
//   console.error("Uncaught exception:");
//   console.error(err && err.stack || err);
//   process.exit(1);
// })

//Winston requires at least one transport (location to save the log) to create a log.
const logConfiguration = {
  transports: [
    new winston.transports.Console()
  ],
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD hh:mm:ss.SSS'
    }),
    winston.format.json()
  ),
  exitOnError: false  //Do not exit on handled exceptions.
}

//Create a logger and pass it the Winston configuration object.
const logger = winston.createLogger(logConfiguration);

/***
Unlike most other programming languages or runtime environments, Node.js doesn't have a built-in
special "main" function to designate the entry point of a program.

Accessing the main module
-------------------------
When a file is run directly from Node.js, require.main is set to its module. That means that it is
possible to determine whether a file has been run directly by testing require.main === module.
***/
if (require.main === module) {
  //Only start the microservice normally if this script is the "main" module.
  main()
  .then(() => {
    READINESS_PROBE = true;
    logger.info(`Microservice is listening on port ${PORT}!`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  })
  .catch(err => {
    logger.error('Microservice failed to start.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  });
}
else {
  //Otherwise, running under test.
  //Will not workas is!!! jct
  module.exports = { startMicroservice }
}

function main() {
  //Throw exception if any required environment variables are missing.
  if (process.env.SVC_DNS_DB === undefined) {
    throw new Error('Please specify the service DNS for the database in the environment variable SVC_DNS_DB.');
  }
  else if (process.env.DB_NAME === undefined) {
    throw new Error('Please specify the name of the database in the environment variable DB_NAME.');
  }
  else if (process.env.SVC_DNS_RABBITMQ === undefined) {
    throw new Error('Please specify the name of the service DNS for RabbitMQ in the environment variable SVC_DNS_RABBITMQ.');
  }
  //Display a message if any optional environment variables are missing.
  else {
    if (process.env.PORT === undefined) {
      logger.info(`The environment variable PORT for the HTTP server is missing; using port ${PORT}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    }
    //
    if (process.env.MAX_RETRIES === undefined) {
      logger.info(`The environment variable MAX_RETRIES is missing; using MAX_RETRIES=${MAX_RETRIES}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    }
  }
  return requestWithRetry(connectToDb, SVC_DNS_DB, MAX_RETRIES)  //Connect to the database...
  .then(dbConn => {                                              //then...
    return requestWithRetry(connectToRabbitMQ, SVC_DNS_RABBITMQ, MAX_RETRIES)  //connect to RabbitMQ...
    .then(conn => {
      //Create a RabbitMQ messaging channel.
      return conn.createChannel();
    })
    .then(channel => {                          //then...
      return startHttpServer(dbConn, channel);  //start the HTTP server.
    });
  });
}

/***
The user IP is determined by the following order:
 1. X-Client-IP
 2. X-Forwarded-For (Header may return multiple IP addresses in the format: "client IP, proxy1 IP, proxy2 IP", so take the the first one.)
    It's very easy to spoof:
    $ curl --header "X-Forwarded-For: 1.2.3.4" "http://localhost:3000"
 3. CF-Connecting-IP (Cloudflare)
 4. Fastly-Client-Ip (Fastly CDN and Firebase hosting header when forwared to a cloud function)
 5. True-Client-Ip (Akamai and Cloudflare)
 6. X-Real-IP (Nginx proxy/FastCGI)
 7. X-Cluster-Client-IP (Rackspace LB, Riverbed Stingray)
 8. X-Forwarded, Forwarded-For and Forwarded (Variations of #2)
 9. req.connection.remoteAddress
10. req.socket.remoteAddress
11. req.connection.socket.remoteAddress
12. req.info.remoteAddress
If an IP address cannot be found, it will return null.
***/
function getIP(req) {
  let ip = null;
  try {
    ip = req.headers['x-forwarded-for']?.split(',').shift() || req.socket?.remoteAddress || null;
    /***
    When the OS is listening with a hybrid IPv4-IPv6 socket, the socket converts an IPv4 address to
    IPv6 by embedding it within the IPv4-mapped IPv6 address format. This format just prefixes the
    IPv4 address with :ffff: (or ::ffff: for older mappings).
    Is the IP an IPv4 address mapped as an IPv6? If yes, extract the Ipv4.
    ***/
    const regex = /^:{1,2}(ffff)?:(?!0)(?!.*\.$)((1?\d?\d|25[0-5]|2[0-4]\d)(\.|$)){4}$/i;  //Ignore case.
    if (ip !== null && regex.test(ip)) {
      ip = ip.replace(/^.*:/, '');
    }
  }
  catch (err) {
    ip = null;
    logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  }
  return ip;
}

//Connect to the database.
function connectToDb(url, currentRetry) {
  logger.info(`Connecting (${currentRetry}) to MongoDB at ${url}/database(${DB_NAME}).`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  return mongodbClient
  .connect(url, { useUnifiedTopology: true })
  .then(client => {
    const db = client.db(DB_NAME);
    logger.info(`Connected to the 'MongoDB' database '${DB_NAME}'.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    //Return an object that represents the database connection.
    return({
      db: db,             //To access the database...
      close: () => {      //and later close the connection to it.
        return client.close();
      }
    });
  });
}

function connectToRabbitMQ(url, currentRetry) {
  logger.info(`Connecting (${currentRetry}) to RabbitMQ at ${url}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  return amqp.connect(url)
  .then(conn => {
    logger.info('Connected to RabbitMQ.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    return conn;
  });
}

function requestWithRetry(func, url, maxRetry, currentRetry = 1) {
  return new Promise(
  (resolve, reject) => {
    //Guaranteed to execute at least once.
    func(url, currentRetry)
    .then(res => {
      resolve(res);
    })
    .catch(err => {
      const timeout = (Math.pow(2, currentRetry) - 1) * 100;
      logger.info(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
      if (++currentRetry > maxRetry) {
        logger.info(`Maximum number of ${maxRetry} retries has been reached.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
        reject(`Maximum number of ${maxRetry} retries has been reached.`);
      }
      else {
        logger.info(`Waiting ${timeout}ms...`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
        setTimeout(() => {
          requestWithRetry(func, url, maxRetry, currentRetry)
          .then(res => resolve(res))
          .catch(err => reject(err));
        }, timeout);
      }
    });
  });
}

//Start the HTTP server.
function startHttpServer(dbConn, channel)
{
  //Notify when server has started.
  return new Promise(resolve => {
    //Create an object to represent a microservice.
    const microservice = {
      db: dbConn.db,
      channel: channel
    };
    app.use(bodyParser.json());  //Enable JSON body for HTTP requests.
    setupHandlers(microservice);
    const server = app.listen(PORT,
    () => {
      //Create a function that can be used to close the server and database.
      microservice.close = () => {
        return new Promise(resolve => {
          server.close(() => {
            resolve();  //Close the Express server.
          });
        })
        .then(() => {
          return dbConn.close();  //Close the database.
        });
      };
      resolve(microservice);
    });
  });
}

//Define the HTTP route handlers here.
function setupHandlers(microservice) {
  //Readiness probe.
  app.get('/readiness',
  (req, res) => {
    res.sendStatus(READINESS_PROBE === true ? 200 : 500);
  });
  //
  const videosCollection = microservice.db.collection("videos");
  //HTTP GET API to retrieve list of videos from the database.
  app.get("/videos",
  (req, res) => {
    /***
    In the HTTP protocol, headers are case-insensitive; however, the Express framework converts
    everything to lower case. Unfortunately, for objects in JavaScript, their property names are
    case-sensitive.
    ***/
    const cid = req.headers['x-correlation-id'];
    //Await the result in the test.
    return videosCollection.find()
    .toArray()  //In a real application this should be paginated.
    .then(videos => {
      logger.info('Retrieved the video collection from the database.', { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      res.json({ videos: videos });
    })
    .catch(err => {
      logger.error('Failed to retrieve the video collection from the database.', { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      res.sendStatus(500);
    });
  });
  //
  //HTTP GET API to retreive details for a particular video.
  app.get("/video",
  (req, res) => {
    const cid = req.headers['x-correlation-id'];
    const videoId = req.query.id;
    logger.info(`Searching in the videos collection for video ${videoId}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    //Await the result in the test.
    return videosCollection.findOne({ _id: videoId })
    .then(video => {
      if (video === undefined) {
        res.sendStatus(404);  //Video with the requested ID doesn't exist!
      }
      else {
        res.json({ video });
      }
    })
    .catch(err => {
      logger.error(`Failed to get video ${videoId}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      res.sendStatus(500);
    });
  });
  //
  //Function to handle incoming messages.
  function consumeUploadedMessage(msg) {
    /***
    Parse the JSON message to a JavaScript object.
    RabbitMQ doesn't natively support JSON. RabbitMQ is actually agnostic about the format for the
    message payload, and from its point of view, a message is just a blob of binary data.
    ***/
    const parsedMsg = JSON.parse(msg.content.toString());
    const cid = parsedMsg.video.cid;
    const videoMetadata = {
      _id: parsedMsg.video.id,
      name: parsedMsg.video.name
    };
    logger.info(`Received an uploaded message: ${videoMetadata._id}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    return videosCollection
    //Record the metadata for the video.
    .insertOne(videoMetadata)
    .then(() => {
      logger.info('Acknowledging message was handled.', { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      microservice.channel.ack(msg);  //If there is no error, acknowledge the message.
    });
  };
  /***
  'Assert' the 'uploaded' exchange rather than the 'uploaded' queue.
  (1) Once connected, 'assert' (not 'create') the message queue and start pulling messages from the
      queue. The different between 'assert' and 'create' the message queue is that multiple
      microservices can 'assert' a queue; i.e., check for the existence of the queue and then only
      creating it when it doesn't already exist. That means the queue is created once and shared
      between all participating microservices.
  (2) Single-recipient messages are one-to-one; a message is sent from one microservice and
      received by only a single microservice. In this configuration, there might be multiple
      senders and receivers, but only one receiver is guaranteed to consume each individual
      message.
  (3) Multi-recipient messages are one-to-many (broadcast or notifications); a message is sent from
      only a single microservice but potentially received by many others. To use multi-recipient
      messages with RabbitMQ, a message exchange is required.
  ***/
  /***
  return microservice.channel.assertExchange('uploaded', 'fanout')
    .then(() =>
    {
      //(1) Create an anonymous queue. The option 'exclusive' is set to 'true' so that the queue
      //    will be deallocated automatically when the microservice disconnects from it; otherwise,
      //    the app will have a memory leak.
      //(2) By creating an unnamed queue, a queue is created uniquely for a microservice. The
      //    'viewed' exchange is shared among all microservices, but the anonymous queue is owned
      //    solely by a microservice.
      //(3) In creating the unnamed queue, a random name generated by RabbitMQ is returned. The
      //    name that RabbitMQ assigned to the queue is only important because the queue must be
      //    binded to the 'viewed' exchange. This binding connects the exchange and the queue, such
      //    that RabbitMQ messages published on the exchange are routed to the queue.
      //(4) Every other microservice that wants to receive the 'viewed' message creates its own
      //    unnamed queue to bind to the 'viewed' exchange. There can be any number of other
      //    microservices bound to the 'viewed' exchange, and they will all receive copies of
      //    messages on their own anonymous queues as messages are published to the exchange.
      return microservice.channel.assertQueue('', { exclusive: true });
    })
    .then(response =>
    {
      //Assign the anonymous queue an automatically generated unique identifier for its name.
      const queueName = response.queue;
      console.log(`Created anonymous queue ${queueName}, binding it to 'uploaded' exchange.`);
      //Bind the queue to the exchange.
      return microservice.channel.bindQueue(queueName, 'uploaded', '')
        .then(() =>
        {
          //Start receiving messages from the anonymous queue that's bound to the 'uploaded'
          //exchange.
          return microservice.channel.consume(queueName, consumeUploadedMessage);
        });
    });
  ***/
  return microservice.channel.assertQueue('uploaded', { exclusive: false })
  .then(() => {
    return microservice.channel.consume('uploaded', consumeUploadedMessage);
  })
}
