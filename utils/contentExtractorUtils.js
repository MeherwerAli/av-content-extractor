const axios = require('axios');
const logger = require('../config/logger');
const { createLogger } = require('winston');

class ContentExtractorUtils {
  static acceptedLanguages = ['en', 'ar', 'fr', 'fa', 'es', 'tr', 'ru', 'zh'];

  static async translateContent(originalContent, langId, translationUrl) {
    try {
      if (!langId || !ContentExtractorUtils.acceptedLanguages.includes(langId)) {
        throw new Error("Invalid language ID");
      }

      if (langId === 'ar') return originalContent;

      logger.info(`Translating the content from ${langId} to ar`);
      const translatedContent = await ContentExtractorUtils.translateSentence(originalContent, langId, 'ar', translationUrl);

      return translatedContent;
    } catch (e) {
      logger.error(`Error while translating the content: ${e.message}`);
      throw new Error(e.message);
    }
  }

  static async sentimentAnalysis(text, sentimentUrl) {
    try {
      if (!text) {
        return 'Neutral';
      }

      const payload = {
        text: text 
      };

      const response = await axios.post(sentimentUrl, payload, {
        headers: {
          'Content-Type': 'application/json',
        },
      });

      return response.data.sentiment || 'Neutral';
    } catch (e) {
      logger.error(`Error while analyzing the sentiment: ${e.message}`);
      return 'Neutral';
    }
  }

  static async translateSentence(sentence, srcLangCode, tgtLangCode, translationServiceUrl) {
    try {
      if (srcLangCode === 'ar') {
        return sentence;
      }
      const payload = {
        text: sentence,
        src_lang_code: srcLangCode,
        tgt_lang_code: tgtLangCode,
      };

      const response = await axios.post(translationServiceUrl, payload, {
        headers: {
          'Content-Type': 'application/json',
        },
      });

      return response.data.translation || '';
    } catch (e) {
      logger.error(`Error while translating sentence: ${e.message}`);
      throw new Error(e.message);
    }
  }

  static async extractData(nerUrl, doc) {
    if (!doc.content || doc.content.trim() === '') {
      return doc; // Return early if content is empty
    }
    const text = `${doc?.title} ${doc.content}`;
    
    try {
      const nerResponse = await ContentExtractorUtils.performNER(
        text,
        ['person', 'location', 'organization'],
        0.3,
        true,
        nerUrl
      );
      const entities = nerResponse?.entities || [];

      // Use Sets to ensure uniqueness and store entities in uppercase
      const locations = new Set();
      const persons = new Set();
      const organizations = new Set();

      entities.forEach(entity => {
        const entityType = entity.entity;
        const word = entity.word.toUpperCase(); // Convert word to uppercase

        switch (entityType) {
          case 'location':
            locations.add(word);
            break;
          case 'person':
            persons.add(word);
            break;
          case 'organization':
            organizations.add(word);
            break;
        }
      });

      // Convert Sets to Arrays for storage
      doc.namedEntitiesLocations_mvs = Array.from(locations);
      doc.namedEntitiesPersons_mvs = Array.from(persons);
      doc.namedEntitiesOrganizations_mvs = Array.from(organizations);

      // Log the unique entities in uppercase
      logger.info(`Locations: ${Array.from(locations)}`);
      logger.info(`Organizations: ${Array.from(organizations)}`);
      logger.info(`Persons: ${Array.from(persons)}`);

      return doc; // Explicitly return the modified document
    } catch (error) {
      console.error('Error performing NER:', error);
      throw error;
    }
  }



  static async performNER(content, types, threshold, filter, nerUrl) {
    try {
      // Construct the data with the correct field names as expected by the server
      const data = JSON.stringify({
        text: content,
        labels: types.join(', '), // Join the array to match "labels" format
        threshold: threshold,
        nested_ner: filter
      });
  
      // Define the axios config similar to Postman
      const config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: `${nerUrl}`, 
        headers: {
          'Content-Type': 'application/json'
        },
        data: data // Use data explicitly formatted as JSON
      };
  
      const response = await axios.request(config);
      return response.data;
    } catch (error) {
      console.error('Error in NER request:', error);
      throw error;
    }
  }
  
}

module.exports = ContentExtractorUtils;
