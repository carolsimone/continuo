# Stage 1: Build
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# Stage 2: Production
FROM node:20-alpine AS production
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/dist-server ./dist-server
COPY proto ./proto
EXPOSE 8090

# node:20-alpine ships the unprivileged "node" user; the server only reads
# /app and listens on 8090.
USER node

CMD ["node", "dist-server/index.js"]
