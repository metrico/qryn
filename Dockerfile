# qryn
FROM node:20-slim

COPY . /app
WORKDIR /app

RUN groupadd -r qryn && useradd -r -g qryn -m qryn && chown -R qryn:qryn /app
USER qryn

RUN npm install --omit=dev

# Expose Ports
EXPOSE 3100

CMD [ "npm", "start" ]
