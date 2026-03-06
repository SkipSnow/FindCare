---
title: ChatHealthyWhoAmIChat
app_file: app.py
sdk: gradio
sdk_version: 5.49.1
---

## Required Variables (Settings → Variables and Secrets)

Add these in your Space's **Settings** → **Variables and Secrets** section:

| Variable Name | Description |
|---------------|-------------|
| `OPENAI_API_KEY` | OpenAI API key for the chat model |
| `MONGO_connectionString` | MongoDB connection string |
| `PUSHOVER_USER` | Pushover user key (for notifications) |
| `PUSHOVER_TOKEN` | Pushover API token |
| `Anthropic_API_KEY` | Anthropic API key (for HIPAA deidentification) |

### MongoDB Atlas (if using Atlas)

**You won't see connection attempts in MongoDB Atlas logs** when the error is a timeout—the request never reaches Atlas. HuggingFace Spaces run from IPs that are not in Atlas's default IP Access List.

**Fix:** In [MongoDB Atlas](https://cloud.mongodb.com) → **Network Access** → **Add IP Address** → add `0.0.0.0/0` to allow connections from anywhere (required for Spaces). For production, consider restricting to known IP ranges.
