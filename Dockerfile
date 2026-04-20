FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json ./package.json
COPY frontend/tsconfig.json ./tsconfig.json
COPY frontend/tsconfig.app.json ./tsconfig.app.json
COPY frontend/tsconfig.node.json ./tsconfig.node.json
COPY frontend/vite.config.ts ./vite.config.ts
COPY frontend/eslint.config.js ./eslint.config.js
COPY frontend/index.html ./index.html
COPY frontend/public ./public
COPY frontend/src ./src
RUN npm install
RUN npm run build

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime
WORKDIR /app
COPY pyproject.toml uv.lock README.md main.py .env.example alembic.ini ./
COPY alembic ./alembic
COPY src ./src
RUN uv sync --no-dev
COPY --from=frontend-build /app/frontend/dist ./frontend/dist
ENV PATH="/app/.venv/bin:${PATH}"
EXPOSE 8000
CMD ["sh", "-lc", "uv run python main.py migrate && uv run python main.py serve --host 0.0.0.0 --port 8000"]
