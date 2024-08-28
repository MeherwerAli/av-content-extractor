const axios = require('axios');
const logger = require('../config/logger');

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

  static async sentimentAnalysis(text, languageId, translationUrl, sentimentUrl) {
    try {
      if (!text || !ContentExtractorUtils.acceptedLanguages.includes(languageId)) {
        return 'Neutral';
      }

      const payload = {
        text: languageId === 'en' ? text : await ContentExtractorUtils.translateSentence(text, languageId, 'en', translationUrl),
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
}

module.exports = ContentExtractorUtils;
