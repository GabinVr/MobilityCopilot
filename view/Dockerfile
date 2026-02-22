# Stage 1: Build
FROM oven/bun:latest AS builder

WORKDIR /app

# Copy package files
COPY package.json bun.lockb* ./

# Install all dependencies (including devDependencies for building)
RUN bun install

# Copy source code
COPY src ./src
COPY public ./public
COPY tsconfig.json ./

# Build the application
RUN bun run build

# Stage 2: Runtime (lightweight)
FROM oven/bun:latest AS runtime

WORKDIR /app

# Create non-root user for security
RUN useradd -m -u 1001 -s /bin/sh app

# Copy package files from builder
COPY --from=builder /app/package.json /app/bun.lockb* ./

# Install only production dependencies
RUN bun install --production

# Copy built application from builder
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/public ./public

# Fix ownership for the app user
RUN chown -R app:app /app

# Switch to non-root user
USER app

# Set environment variables
ENV NODE_ENV=production

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD bun run --no-save -e "const res = await fetch('http://localhost:3000'); process.exit(res.ok ? 0 : 1);" || exit 1

# Start application from built dist
CMD ["bun", "run", "dist/index.js"]
