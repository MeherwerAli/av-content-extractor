const contentExtractorUtils = require('../utils/contentExtractorUtils');
const logger = require('../config/logger');

class AvMapper {
  static async mapToAvDoc(message, translationUrl, sentimentUrl) {
    try {
      const avDoc = JSON.parse(message);

      avDoc.translation = await contentExtractorUtils.translateContent(avDoc.content, avDoc.languageId, translationUrl);
      avDoc.titleTranslation = await contentExtractorUtils.translateContent(avDoc.title, avDoc.languageId, translationUrl);
      avDoc.contentSentiment = await contentExtractorUtils.sentimentAnalysis(avDoc.content, avDoc.languageId, translationUrl, sentimentUrl);

      return avDoc;
    } catch (e) {
      logger.error(`Error while mapping to AVDoc: ${e.message}`);
      throw new Error(e);
    }
  }
}

module.exports = AvMapper;
