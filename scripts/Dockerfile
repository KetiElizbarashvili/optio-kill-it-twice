FROM node:18-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY seed.js geo.js ./
ENTRYPOINT ["node", "seed.js"]
