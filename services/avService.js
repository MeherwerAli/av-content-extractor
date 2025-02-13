const { Kafka } = require('kafkajs');
const avMapper = require('../mappers/avMapper');
const elasticsearchClient = require('../config/elasticsearchClient');
const logger = require('../config/logger');

class AVService {
  constructor(translationUrl, translationEnUrl, sentimentUrl, nerUrl, concurrencyLimit = 20) {
    this.translationUrl = translationUrl;
    this.translationEnUrl = translationEnUrl;
    this.sentimentUrl = sentimentUrl;
    this.nerUrl = nerUrl;
    this.podId = process.env.HOSTNAME || 'defaultPod';

    this.kafka = new Kafka({
      clientId: 'content-extractor-service',
      brokers: process.env.KAFKA_BROKERS.split(',')
    });

    const groupId = `avdoc-group-${this.podId}`;
    this.consumer = this.kafka.consumer({
      groupId: groupId,
      autoCommit: false,
      autoOffsetReset: 'earliest',
      sessionTimeout: 600000,
      heartbeatInterval: 3000,
      maxPollInterval: 300000
    });

    this.concurrencyLimit = concurrencyLimit;
  }

  async processMessage(message, topicName) {
    try {
      const avDoc = JSON.parse(message);

      if (!avDoc.timeDelimStart || !avDoc.timeDelimEnd) {
        logger.warn("Skipping invalid message: timeDelimStart or timeDelimEnd is missing.");
        return;
      }

      const docId = `${avDoc.sourceId}-${avDoc.timeDelimStart}-${avDoc.timeDelimEnd}`;
      let existingDoc;

      try {
        existingDoc = await elasticsearchClient.get({
          index: 'av_docs',
          id: docId
        });
        if (existingDoc && existingDoc.found) {
          logger.info(`Document with ID ${docId} already exists. Skipping.`);
          return;
        }
      } catch (error) {
        if (error.meta && error.meta.statusCode === 404) {
          logger.info(`Document with ID ${docId} not found, proceeding to create.`);
        } else {
          logger.error(`Error checking document existence: ${error.message}`);
          return; // Log and skip this message
        }
      }

      avDoc.connector = "AVConnector";
      avDoc.translation = "";
      avDoc.titleTranslation = "";
      avDoc.contentSentiment = "Neutral";
      avDoc.id = docId;

      logger.info("Consuming new message");
      if (avDoc.content && avDoc.content.length > 2000) {
        logger.info("Processing info directly");
        await this.pushToElasticsearch(avDoc, docId);
        // Perform additional processing asynchronously
        this.enrichDocumentAsync(avDoc, docId);
      } else {
        let avDocMapped;
        if (avDoc.languageId === 'en') {
          avDocMapped = await avMapper.mapToAvDoc(avDoc, this.translationEnUrl, this.sentimentUrl, this.nerUrl);
        } else {
          avDocMapped = await avMapper.mapToAvDoc(avDoc, this.translationUrl, this.sentimentUrl, this.nerUrl);
        }
        await this.pushToElasticsearch(avDocMapped, docId);
      }
    } catch (error) {
      logger.error(`Failed to process message from topic ${topicName}: ${error.message}`);
    }
  }

  async enrichDocumentAsync(avDoc, docId) {
    try {
      let enrichedDoc;
      if (avDoc.languageId === 'en'){
        enrichedDoc = await avMapper.mapToAvDoc(
          avDoc,
          this.translationEnUrl,
          this.sentimentUrl,
          this.nerUrl
        );
      } else {
        enrichedDoc = await avMapper.mapToAvDoc(
          avDoc,
          this.translationUrl,
          this.sentimentUrl,
          this.nerUrl
        );
      }
  
      // Update Elasticsearch document with the enriched data
      await this.updateElasticsearchDocument(enrichedDoc, docId);
    } catch (error) {
      logger.error(`Error enriching and updating document ${docId}: ${error.message}`);
    }
  }

  async pushToElasticsearch(avDoc, docId) {
    try {
      logger.info("Pushing document to Elasticsearch");
      await elasticsearchClient.index({
        index: 'av_docs',
        id: docId,
        body: avDoc
      });

      logger.info(`Indexed document with id: ${docId}`);
    } catch (error) {
      logger.error(`Error while pushing to Elasticsearch: ${error.message}`);
    }
  }

  async updateElasticsearchDocument(enrichedDoc, docId) {
    try {
      await elasticsearchClient.update({
        index: 'av_docs',
        id: docId,
        body: {
          doc: enrichedDoc
        }
      });
  
      logger.info(`Updated document with id: ${docId}`);
    } catch (error) {
      logger.error(`Error updating document in Elasticsearch: ${error.message}`);
    }
  }

  async processBatch(messages, topicName, partition) {
    const batchSize = Math.min(this.concurrencyLimit, messages.length);
    let index = 0;

    while (index < messages.length) {
      const batchEnd = Math.min(index + batchSize, messages.length);
      const currentBatch = messages.slice(index, batchEnd);

      for (let i = 0; i < currentBatch.length; i++) {
        const message = currentBatch[i];
        try {
          await this.processMessage(
            message.value.toString(),
            topicName
          );
        } catch (error) {
          logger.error(`Error processing message from batch: ${error.message}`);
        }
      }

      index += batchSize;
    }
  }

  async start() {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: 'av-scrapper-topic', fromBeginning: false });

      await this.consumer.run({
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
          const { topic, partition } = batch;

          logger.info(`Processing batch of ${batch.messages.length} messages on partition ${partition}`);
          try {
            await this.processBatch(batch.messages, topic, partition);

            for (let i = 0; i < batch.messages.length; i++) {
              const message = batch.messages[i];
              resolveOffset(message.offset);
            }
          } catch (error) {
            logger.error(`Failed to process batch on topic ${topic}, partition ${partition}: ${error.message}`);
          }

          if (!isRunning() || isStale()) {
            logger.warn('Consumer is no longer running or has become stale');
            return;
          }
          await heartbeat();
          await this.consumer.commitOffsets([
            { topic: batch.topic, partition: batch.partition, offset: (parseInt(batch.lastOffset(), 10) + 1).toString() }
          ]);
        }
      });
    } catch (error) {
      logger.error(`Error in Kafka consumer: ${error.message}`);

      if (error.message.includes('The coordinator is not aware of this member')) {
        logger.info('Attempting to reinitialize consumer...');
        await this.consumer.disconnect();
        await this.start();
      } else {
        logger.error('Consumer encountered a critical error and stopped processing.');
      }
    }
  }
}

module.exports = AVService;
