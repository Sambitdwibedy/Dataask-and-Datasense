FROM node:20-alpine

# Install Python3 (needed for SQLite data extraction in data-loader.js)
RUN apk add --no-cache python3

WORKDIR /app

# Copy server package files
COPY server/package.json server/package-lock.json* ./server/

# Install production dependencies
WORKDIR /app/server
RUN npm install --production

# Copy all app files
WORKDIR /app
COPY . .

# Reassemble split files (e.g., codebase_community.sqlite.gz was split to stay under GitHub 100MB limit)
RUN for base in data/*.sqlite.gz.partaa; do \
      if [ -f "$base" ]; then \
        target="${base%.partaa}"; \
        cat "${target}.part"* > "$target"; \
        rm -f "${target}.part"*; \
        echo "Reassembled $(basename "$target")"; \
      fi; \
    done

# Decompress SQLite data files if they exist
# NOTE: Alpine uses busybox gunzip which does NOT support -k flag.
# Use zcat (busybox-compatible) to decompress while keeping the .gz original.
RUN for gz in data/*.sqlite.gz; do \
      if [ -f "$gz" ]; then \
        outfile="${gz%.gz}"; \
        zcat "$gz" > "$outfile"; \
        echo "Decompressed $(basename "$outfile") ($(du -h "$outfile" | cut -f1))"; \
      fi; \
    done

# Create uploads directory
RUN mkdir -p /app/uploads

# Expose port (Railway overrides via PORT env var)
EXPOSE 3002

# Start server with increased heap (default 512MB is too low for pgvector embeddings)
WORKDIR /app/server
CMD ["node", "--max-old-space-size=2048", "index.js"]
