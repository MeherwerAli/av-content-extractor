const contentExtractorUtils = require('../utils/contentExtractorUtils');
const logger = require('../config/logger');

class AvMapper {
  static async mapToAvDoc(avDoc, translationUrl, sentimentUrl) {
    try {

      logger.info("Translating content");
      avDoc.translation = await contentExtractorUtils.translateContent(avDoc.content, avDoc.languageId, translationUrl);

      logger.info("Translating title");
      avDoc.titleTranslation = await contentExtractorUtils.translateContent(avDoc.title, avDoc.languageId, translationUrl);
      logger.info("Doing Sentiment analysis ");
      avDoc.contentSentiment = await contentExtractorUtils.sentimentAnalysis(avDoc.title, avDoc.languageId, translationUrl, sentimentUrl);
      avDoc.connector = "AVConnector";
      logger.info("Mapping complete");
      return avDoc;
    } catch (e) {
      logger.error(`Error while mapping to AVDoc: ${e.message}`);
      throw new Error(e);
    }
  }
}

module.exports = AvMapper;
