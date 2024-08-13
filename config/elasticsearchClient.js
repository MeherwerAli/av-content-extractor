const { Client } = require('@elastic/elasticsearch');
const dotenv = require('dotenv');

const logger = require('../config/logger');

// Load environment variables from the .env file
dotenv.config({ path: './config/config.env' });

const elasticsearchHosts = process.env.ELASTICSEARCH_HOST;

const client = new Client({
  nodes: elasticsearchHosts,
  maxRetries: 5,
  requestTimeout: 60000,
  sniffOnStart: true,
  logger,
});

module.exports = client;
