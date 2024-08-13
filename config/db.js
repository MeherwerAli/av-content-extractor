const mongoose = require('mongoose');
const dotenv = require('dotenv');

// Load environment variables from the .env file
dotenv.config({ path: './config/config.env' });

const connectDB = async () => {
  try {
    mongoose.set('strictQuery', true);
    const connection = await mongoose.connect(
        buildMongoUri(),
        {}
    );
    console.log(
        `Mongodb connected: ${connection.connection.host} to db ${connection.connection.name}`.bold.blue
    );
} catch (error) {
    console.error(`Error while connecting to MongoDB: ${error.message}`.red.bold);
}
};

const buildMongoUri = () => {
  // Use credentials and other details from environment variables
  const username = encodeURIComponent(process.env.MONGO_USERNAME);
  const password = encodeURIComponent(process.env.MONGO_PASSWORD);
  const host = process.env.MONGO_HOSTNAME;
  const port = process.env.MONGO_PORT;
  const database = process.env.MONGO_DATABASE;

  const connectionString = `mongodb://${username}:${password}@${host}:${port}/${database}?authSource=admin`;
  console.log("connectionString: ".blue, connectionString);
  return connectionString;
};

module.exports = connectDB;
