const { Kafka } = require('kafkajs');
const avMapper = require('../mappers/avMapper');
const elasticsearchClient = require('../config/elasticsearchClient');
const logger = require('../config/logger');

class AVService {
  constructor(translationUrl, sentimentUrl) {
    this.translationUrl = translationUrl;
    this.sentimentUrl = sentimentUrl;
    this.kafka = new Kafka({
      clientId: 'content-extractor-service',
      brokers: process.env.KAFKA_BROKERS.split(','), // Ensure this is set correctly in your .env file
    });

    this.consumer = this.kafka.consumer({ groupId: 'avdoc-group' });
  }

  async processAvDocMessage(message, topicName) {
    try {
      const avDoc = await avMapper.mapToAvDoc(message, this.translationUrl, this.sentimentUrl);
      logger.info(`Consumed message from topic ${topicName}: ${JSON.stringify(avDoc)}`);
      await this.pushToElasticsearch(avDoc);
    } catch (e) {
      logger.error(`Failed to process message from topic ${topicName}: ${e.message}`);
    }
  }

  async pushToElasticsearch(avDoc) {
    try {
      const response = await elasticsearchClient.index({
        index: 'av_docs',
        body: avDoc,
      });
      logger.info(`Indexed document with id: ${response.body._id}`);
    } catch (e) {
      logger.error(`Error while pushing to Elasticsearch: ${e.message}`);
      throw new Error(e);
    }
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'av-scrapper-topic', fromBeginning: false }); // Subscribes to the 'av-scrapper-topic'

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const decodedMessage = message.value.toString();
        await this.processAvDocMessage(decodedMessage, topic);
      },
    });
  }
}

module.exports = AVService;
