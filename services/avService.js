const { Kafka } = require('kafkajs');
const avMapper = require('../mappers/avMapper');
const elasticsearchClient = require('../config/elasticsearchClient');
const logger = require('../config/logger');
const pLimit = require('p-limit');  // Concurrency limiter

class AVService {
  constructor(translationUrl, sentimentUrl, nerUrl, concurrencyLimit = 20) {  // Set a concurrency limit
    this.translationUrl = translationUrl;
    this.sentimentUrl = sentimentUrl;
    this.nerUrl = nerUrl;

    this.kafka = new Kafka({
      clientId: 'content-extractor-service',
      brokers: process.env.KAFKA_BROKERS.split(',')
    });

    this.consumer = this.kafka.consumer({
      groupId: 'avdoc-group',
      autoCommit: false,
      autoOffsetReset: 'earliest',
      sessionTimeout: 60000,           // Increased to reduce rebalance frequency
      heartbeatInterval: 5000,         // Regular heartbeats to prevent timeouts
      maxPollInterval: 300000          // Matches long-running batch process
    });

    this.concurrencyLimit = pLimit(concurrencyLimit); // Set concurrency limit for API calls
  }

  async processMessage(message, topicName) {
    const avDoc = JSON.parse(message);
    
    if (!avDoc.timeDelimStart || !avDoc.timeDelimEnd) {
      logger.warn("Skipping invalid message: timeDelimStart or timeDelimEnd is missing.");
      return;
    }

    // Check for existing document
    const existingDoc = await elasticsearchClient.search({
      index: 'av_docs',
      body: {
        query: {
          bool: {
            must: [
              { match: { timeDelimStart: avDoc.timeDelimStart } },
              { match: { timeDelimEnd: avDoc.timeDelimEnd } },
              { match: { sourceId: avDoc.sourceId } },
            ]
          }
        }
      }
    });

    if (existingDoc.hits.total.value > 0) {
      logger.info(`Document with sourceId >> ${avDoc.sourceId}, timeDelimStart ${avDoc.timeDelimStart} and timeDelimEnd ${avDoc.timeDelimEnd} already exists. Skipping.`);
      return;
    }

    // Process mapping with external APIs (translation, sentiment, etc.)
    const avDocMapped = await avMapper.mapToAvDoc(avDoc, this.translationUrl, this.sentimentUrl, this.nerUrl);
    await this.pushToElasticsearch(avDocMapped);
  }

  async processBatch(messages, topicName) {
    const processPromises = messages.map(({ value }) =>
      this.concurrencyLimit(() => this.processMessage(value.toString(), topicName))
    );

    await Promise.all(processPromises);
  }

  async pushToElasticsearch(avDoc) {
    try {
      logger.info("Pushing document to Elasticsearch");

      const response = await elasticsearchClient.index({
        index: 'av_docs',
        body: avDoc
      });

      if (response && response._id) {
        logger.info(`Indexed document with id: ${response._id}`);
        avDoc.id = response._id;

        await elasticsearchClient.update({
          index: 'av_docs',
          id: avDoc.id,
          body: { doc: avDoc }
        });
      }
    } catch (error) {
      logger.error(`Error while pushing to Elasticsearch: ${error.message}`);
      throw error;
    }
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'av-scrapper-topic', fromBeginning: false });

    await this.consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        logger.info(`Processing batch of ${batch.messages.length} messages`);

        await this.processBatch(batch.messages, batch.topic);

        for (const message of batch.messages) {
          resolveOffset(message.offset); // Mark the message as processed
        }

        await heartbeat();
        await this.consumer.commitOffsets([
          { topic: batch.topic, partition: batch.partition, offset: (parseInt(batch.lastOffset(), 10) + 1).toString() }
        ]);
      },
    });
  }
}

module.exports = AVService;
