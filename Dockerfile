# Use a specific version of Node
FROM node:latest

# Set the working directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./
RUN npm install

# Bundle app source
COPY . .

# Your app binds to port 3000, so use the EXPOSE instruction to have it mapped
EXPOSE 8096

# Define the command to run your app
CMD [ "npm", "start" ]
