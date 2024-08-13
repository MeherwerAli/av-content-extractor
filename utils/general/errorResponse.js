const { standardResponse } = require('../standardResponse');

class ErrorResponse extends Error {
  constructor(message, statusCode) {
    super(message);
    this.statusCode = statusCode;
    // Error.captureStackTrace(this, this.constructor);
  }
  send(res) {
    standardResponse(res, this.message, this.statusCode, null, this, false);
  }
}
module.exports = ErrorResponse;
