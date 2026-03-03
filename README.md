# FPL Tool

A free, persistent Fantasy Premier League tool for you and your friends.

## Setup

### 1. Database (Neon)
- Create a Neon project and copy the connection string.
- Run the SQL from [Database Schema](#) to create tables.

### 2. GitHub Repository
- Push this code to a **public** repo.
- Add `DATABASE_URL` as a repository secret (Settings → Secrets and variables → Actions).

### 3. Deploy on Streamlit Cloud
- Connect your GitHub repo to [share.streamlit.io](https://share.streamlit.io).
- Add the same `DATABASE_URL` in the app's **Secrets** section (as TOML):
  ```toml
  [passwords]
  DATABASE_URL = "postgresql://..."