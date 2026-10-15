# Production Dockerfile for Kafka Application
FROM node:20-alpine AS runner

WORKDIR /app
COPY package.json ./
RUN npm install

COPY . .

EXPOSE 3000

CMD ["npm", "start"]
