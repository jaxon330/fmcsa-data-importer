FROM node:lts-bookworm-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts
COPY migrations ./migrations
COPY README.md ./

RUN npm run build

CMD ["node", "-e", "console.log('FMCSA data importer container ready. Override the command with npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history')"]
