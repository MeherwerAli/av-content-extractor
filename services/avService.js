const { Kafka } = require('kafkajs');
const avMapper = require('../mappers/avMapper');
const elasticsearchClient = require('../config/elasticsearchClient');
const logger = require('../config/logger');



class AVService {
  constructor(translationUrl, sentimentUrl) {
    this.translationUrl = translationUrl;
    this.sentimentUrl = sentimentUrl;
    console.log("KAFA Broker ", process.env.KAFKA_BROKERS);
    this.kafka = new Kafka({
      clientId: 'content-extractor-service',
      brokers: process.env.KAFKA_BROKERS.split(',')
    });

    this.consumer = this.kafka.consumer({
      groupId: 'avdoc-group',
      autoCommit: true,
      autoOffsetReset: 'earliest'  // Start from the earliest offset if no offset is committed
    });
  }

  async processAvDocMessage(message, topicName) {
    try {

      const avDoc = JSON.parse(message);


      if (!avDoc.timeDelimStart || !avDoc.timeDelimEnd) {
        throw new Error("Invalid message: timeDelimStart or timeDelimEnd is missing.");
      }

      // Check if the document with the same timeDelimStart and timeDelimEnd already exists
      const existingDoc = await elasticsearchClient.search({
        index: 'av_docs',
        body: {
          query: {
            bool: {
              must: [
                { match: { timeDelimStart: avDoc.timeDelimStart } },
                { match: { timeDelimEnd: avDoc.timeDelimEnd } }
              ]
            }
          }
        }
      });

      // If a document exists, log a message and return
      if (existingDoc.hits.total.value > 0) {
        logger.info(`Document with timeDelimStart ${avDoc.timeDelimStart} and timeDelimEnd ${avDoc.timeDelimEnd} already exists. Skipping indexing.`);
        return;
      }
      logger.info("Consuming new message");
      const avDocMapped = await avMapper.mapToAvDoc(avDoc, this.translationUrl, this.sentimentUrl);
      logger.info(`Consumed message from topic ${topicName}: ${JSON.stringify(avDocMapped)}`);
      await this.pushToElasticsearch(avDocMapped);
    } catch (e) {
      logger.error(`Failed to process message from topic ${topicName}: ${e.message}`);
      throw e;
    }
  }

  async pushToElasticsearch(avDoc) {
    try {

      logger.info("Pushing to elastic Search");
      // If no document exists, proceed to index the new document
      const response = await elasticsearchClient.index({
        index: 'av_docs',
        body: avDoc,
      });

      // Log the entire response for debugging purposes
      logger.info(`Elasticsearch response: ${JSON.stringify(response)}`);

      // Check if the response contains the '_id' field
      if (response && response._id) {
        logger.info(`Indexed document with id: ${response._id}`);
        // Retrieve the Elasticsearch _id and add it as a new field 'id' in the document
        const documentId = response._id;
        avDoc.id = documentId;

        // Update the document in Elasticsearch with the new 'id' field
        await elasticsearchClient.update({
          index: 'av_docs',
          id: documentId,
          body: {
            doc: avDoc
          }
        });
      }

    } catch (e) {
      logger.error(`Error while pushing to Elasticsearch: ${e.message}`);
      throw new Error(e);
    }
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'av-scrapper-topic', fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`Received message`);
        try {
          const decodedMessage = message.value.toString();
          await this.processAvDocMessage(decodedMessage, topic);
        } catch (error) {
          console.error(`Error processing message: ${error.message}`, error);
        }
      },
    });
  }
}

module.exports = AVService;
