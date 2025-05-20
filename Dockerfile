FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm install kuzu --platform=linux --force
RUN npm install --force && npm cache clean --force
COPY . .
EXPOSE 8002
CMD ["node", "index.js", "./kuzu_db"]