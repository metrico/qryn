# cLoki
FROM node:8

# BUILD FORCE
ENV BUILD 703022

COPY . /app
WORKDIR /app
RUN npm install

# Expose Ports
EXPOSE 80

CMD [ "npm", "start" ]
