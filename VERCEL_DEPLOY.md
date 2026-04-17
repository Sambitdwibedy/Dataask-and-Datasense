# Vercel Deployment Guide — IDA Data Ask

## What was fixed (pre-deployment prep)

| File | Change |
|------|--------|
| `package.json` (root) | **Created** — Vercel needs this to detect Node ≥18 and install deps |
| `vercel.json` | Updated with `maxDuration`, `memory`, `NODE_ENV=production`, and `includeFiles` |
| `server/db.js` | SSL config now works with **Neon, Supabase, Vercel Postgres, Railway** (was Railway-only) |
| `server/index.js` | Added missing `pipeline-steps` route (`/api/pipeline-steps`) |

---

## Step-by-step Vercel deployment

### 1. Push code to GitHub
```bash
git add .
git commit -m "chore: Vercel deployment prep"
git push origin main
```

### 2. Create a PostgreSQL database
Pick one of:
- **Vercel Postgres** (Storage tab in Vercel dashboard — easiest)
- **Neon** (neon.tech — free tier)
- **Supabase** (supabase.com — free tier)
- Your existing **Railway** database URL

### 3. Import to Vercel
1. Go to [vercel.com/new](https://vercel.com/new)
2. Import the GitHub repo
3. Leave **Framework Preset** as "Other"
4. Leave **Root Directory** as `.` (root)
5. Click **Deploy** — Vercel auto-detects `vercel.json`

### 4. Set Environment Variables
In **Project → Settings → Environment Variables**, add:

| Variable | Value |
|----------|-------|
| `DATABASE_URL` | Your PostgreSQL connection string |
| `JWT_SECRET` | Strong random string (`openssl rand -hex 32`) |
| `ANTHROPIC_API_KEY` | `sk-ant-...` |
| `OPENAI_API_KEY` | `sk-...` |
| `CORS_ORIGIN` | `https://your-app.vercel.app` (set after first deploy) |

### 5. Initialize the database
After first deploy, run the schema + seed against your cloud Postgres:
```bash
# From your local machine (with DATABASE_URL set in .env)
cd server
node init-db.js
```

### 6. Verify the deployment
- `https://your-app.vercel.app/api/health` → `{ "status": "ok" }`
- `https://your-app.vercel.app/api/diag` → shows table row counts + pgvector status

---

## Known Vercel limitations to be aware of

**File uploads are ephemeral** — Vercel serverless functions use a temporary filesystem. Uploaded files (PDFs, Excel, etc.) survive only for the duration of the request. For persistent uploads, configure an S3 / Cloudflare R2 / Vercel Blob bucket and update `server/routes/documents.js` to store there.

**Function timeout** — `vercel.json` sets `maxDuration: 60s`. Long-running AI enrichment pipelines may time out. Consider splitting those into background jobs or using Vercel Cron.

**Cold starts** — The first request after inactivity will be slower (~2–3 s). This is normal for serverless.

---

## Project structure overview

```
/
├── package.json          ← Root manifest (node ≥18 engine spec)
├── vercel.json           ← Vercel build + routing config
├── .vercelignore         ← Files excluded from the Vercel build
├── server/
│   ├── index.js          ← Express entry point (the Vercel lambda)
│   ├── package.json      ← All npm dependencies
│   ├── db.js             ← PostgreSQL pool (SSL auto-detected)
│   ├── .env.example      ← Environment variable reference
│   ├── client/
│   │   └── index.html    ← Single-page React frontend (served as static)
│   ├── routes/           ← API route handlers
│   ├── services/         ← LLM, embedding, intent, query services
│   └── middleware/
│       └── auth.js       ← JWT middleware
└── test-docs/            ← Sample policy docs for testing the pipeline
```
