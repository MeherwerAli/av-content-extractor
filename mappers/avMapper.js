const contentExtractorUtils = require('../utils/contentExtractorUtils');
const logger = require('../config/logger');

class AvMapper {
  static async mapToAvDoc(avDoc, translationUrl, sentimentUrl, nerUrl) {
    try {
      logger.info("Starting parallel processing for content translation, title translation, sentiment analysis, and NER");

      // Initiate all operations simultaneously and wait for them to complete
      const [translation, titleTranslation, sentiment] = await Promise.all([
        contentExtractorUtils.translateContent(avDoc.content, avDoc.languageId, translationUrl)
          .then((result) => {
            logger.info("Content translation completed");
            return result;
          }),
        contentExtractorUtils.translateContent(avDoc.title, avDoc.languageId, translationUrl)
          .then((result) => {
            logger.info("Title translation completed");
            return result;
          }),
        contentExtractorUtils.sentimentAnalysis(avDoc.content, sentimentUrl)
          .then((result) => {
            logger.info("Sentiment analysis completed");
            return result;
          }),
        contentExtractorUtils.extractData(nerUrl, avDoc)
          .then(() => {
            logger.info("Named Entity Recognition (NER) completed");
          })
      ]);

      // Assign results to avDoc
      avDoc.translation = translation;
      avDoc.titleTranslation = titleTranslation;
      avDoc.contentSentiment = sentiment;

      logger.info("Mapping from AI complete");
      return avDoc;

    } catch (e) {
      logger.error(`Error while mapping to AVDoc: ${e.message}`);
      throw new Error(e);
    }
  }
}

module.exports = AvMapper;
