const { Client } = require('@elastic/elasticsearch');
const { logger } = require('./logger');
require('dotenv').config();

const elasticsearchHosts = process.env.ELASTICSEARCH_HOSTS.split(',');

const client = new Client({
  nodes: elasticsearchHosts,
  maxRetries: 5,
  requestTimeout: 60000,
  sniffOnStart: true,
  logger,
});

module.exports = client;
