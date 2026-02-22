FROM oven/bun:latest

WORKDIR /app

# Copy package files
COPY package.json bun.lockb* ./

# Install dependencies
RUN bun install --production

# Copy application code
COPY src ./src
COPY public ./public
COPY tsconfig.json ./

# Use .env for environment variables
COPY .env ./

# Set env variables for production
ENV NODE_ENV=production

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD bun run health-check || exit 1

# Start application
CMD ["bun", "run", "src/index.ts"]
