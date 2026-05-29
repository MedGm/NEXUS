FROM node:20-alpine

WORKDIR /app

COPY dashboard/package*.json ./
RUN npm ci --prefer-offline

COPY dashboard/ .

ENV NODE_ENV=development
CMD ["npm", "run", "dev"]
