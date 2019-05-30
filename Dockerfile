# cLoki
FROM node:8

# BUILD FORCE
ENV BUILD 703024
ENV PORT 3100
ENV CLOKI_LOGIN "logger"
ENV CLOKI_PASSWORD "password"

COPY . /app
WORKDIR /app
RUN npm install

# Expose Ports
EXPOSE 3100

CMD [ "npm", "start" ]
