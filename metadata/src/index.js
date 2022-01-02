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

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_NAME = "metadata";
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
  logger.error(`${SVC_NAME} - Uncaught exception.`);
  logger.error(`${SVC_NAME} - ${err}`);
  logger.error(`${SVC_NAME} - ${err.stack}`);
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
  transports: [ new winston.transports.Console() ],
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSSSS' }),
    winston.format.printf(msg => `${msg.timestamp} ${msg.level} ${msg.message}`)
  ),
  exitOnError: false
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
    logger.info(`${SVC_NAME} - Microservice is listening on port "${PORT}"!`);
  })
  .catch(err => {
    logger.error(`${SVC_NAME} - Microservice failed to start.`);
    logger.error(`${SVC_NAME} - ${err}`);
    logger.error(`${SVC_NAME} - ${err.stack}`);
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
      logger.info(`${SVC_NAME} - The environment variable PORT for the HTTP server is missing; using port ${PORT}.`);
    }
    //
    if (process.env.MAX_RETRIES === undefined) {
      logger.info(`${SVC_NAME} - The environment variable MAX_RETRIES is missing; using MAX_RETRIES=${MAX_RETRIES}.`);
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

//Connect to the database.
function connectToDb(url, currentRetry) {
  logger.info(`${SVC_NAME} - Connecting (${currentRetry}) to 'MongoDB' at ${url}/database(${DB_NAME}).`);
  return mongodbClient
  .connect(url, { useUnifiedTopology: true })
  .then(client => {
    const db = client.db(DB_NAME);
    logger.info(`${SVC_NAME} - Connected to mongodb database '${DB_NAME}'.`);
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
  logger.info(`${SVC_NAME} - Connecting (${currentRetry}) to 'RabbitMQ' at ${url}.`);
  return amqp.connect(url)
  .then(conn => {
    logger.info(`${SVC_NAME} - Connected to RabbitMQ.`);
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
      logger.info(`${SVC_NAME} - ${err}`);
      if (++currentRetry > maxRetry) {
        reject(`Maximum number of ${maxRetry} retries has been reached.`);
      }
      else {
        logger.info(`${SVC_NAME} - Waiting ${timeout}ms...`);
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
    const cid = req.headers['X-Correlation-Id'];
    //Await the result in the test.
    return videosCollection.find()
    .toArray()  //In a real application this should be paginated.
    .then(videos => {
      logger.info(`${SVC_NAME} ${cid} - Retrieved the video collection from the database.`);
      res.json({ videos: videos });
    })
    .catch(err => {
      logger.error(`${SVC_NAME} ${cid} - Failed to retrieve the video collection from the database.`);
      logger.error(`${SVC_NAME} ${cid} - ${err}`);
      res.sendStatus(500);
    });
  });
  //
  //HTTP GET API to retreive details for a particular video.
  app.get("/video",
  (req, res) => {
    const cid = req.headers['X-Correlation-Id'];
    const videoId = req.query.id;
    logger.info(`${SVC_NAME} ${cid} - Searching in the "videos" collection for video ${videoId}`);
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
      logger.error(`${SVC_NAME} ${cid} - Failed to get video ${videoId}.`);
      logger.error(`${SVC_NAME} ${cid} - ${err}`);
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
    logger.info(`${SVC_NAME} ${cid} - Received an "uploaded" message: ${videoMetadata._id}-${videoMetadata.name}`);
    return videosCollection
    //Record the metadata for the video.
    .insertOne(videoMetadata)
    .then(() => {
      logger.info(`${SVC_NAME} ${cid} - Acknowledging message was handled.`);
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
