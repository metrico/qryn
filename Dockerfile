# qryn
FROM node:20-slim

COPY . /app
WORKDIR /app
RUN npm install

# Expose Ports
EXPOSE 3100

CMD [ "npm", "start" ]
