# cLoki
FROM node:14-slim

# Build ENV
ENV BUILD 703033
ENV PORT 3100

# Build App
COPY . /app
WORKDIR /app
RUN npm install
RUN npm install pm2 -g

# Expose Ports
EXPOSE 3100

# Use PM2 ecosystem init
CMD ["pm2-runtime", "ecosystem.config.js"]
