//Essential
const path = require('path');
const dotenv = require('dotenv');
const AVService = require('./services/avService');
const logger = require('./config/logger');

if (process.env.NODE_ENV !== 'production') {
    dotenv.config({ path: path.join(__dirname, '..', 'config', 'config.env') });
}

const translationUrl = process.env.TRANSLATION_URL || 'http://172.181.60:5001/translate';
const sentimentUrl = process.env.SENTIMENT_URL || 'http://172.181.60:5002/analyze';

const avService = new AVService(translationUrl, sentimentUrl);

avService.start().then(() => {
  logger.info('Kafka listener started and listening to av-scrapper-topic.');
}).catch((err) => {
  logger.error(`Error starting Kafka listener: ${err.message}`);
});

process.on('unhandledRejection', (err, promise) => {
    console.log(`Error: ${err.message}`.red.bold);
    index.close(() => process.exit(1));
});

