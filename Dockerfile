FROM node:20-alpine

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache curl

# Copy dependency files
COPY package*.json tsconfig.json ./

# Install all dependencies including devDependencies for tests and scripts
RUN npm install

# Copy application source code
COPY . .

# Build TypeScript to dist
RUN npm run build

# Make entrypoint script executable
RUN chmod +x entrypoint.sh

EXPOSE 3000

ENTRYPOINT ["./entrypoint.sh"]
