# Publish to GitHub (one-time remote repository setup)

The GitHub connection supports writing to an **existing** repository but does not expose an action to **create** a new repository. Consequently, this source package is locally prepared, not claimed to have been pushed to GitHub.

## 1. Create your empty repository

Open **https://github.com/new** in a browser logged into your account. Choose:

- Owner: `Manaswi-Guntupalli`
- Repository: `distributed-search-engine-kafka`
- Visibility: **Private** initially, or Public if you and your team wish to share it
- Do **not** initialize with a README, .gitignore or license (this project already has a README and .gitignore)

Then click **Create repository**.

## 2. Push the project files

From an extracted copy of `distributed-search-engine-kafka.zip` in your terminal:

```bash
cd distributed-search-engine-kafka
# First-time git setup: configure your OWN identity if git asks.
git config --global user.name "Your GitHub Display Name"
git config --global user.email "you@example.com"  # change this; use GitHub no-reply address if preferred

git init
git add .
git commit -m "Initial distributed search engine with Kafka integration"
git branch -M main
git remote add origin https://github.com/Manaswi-Guntupalli/distributed-search-engine-kafka.git
git push -u origin main
```

Use your GitHub credential manager, SSH key or GitHub CLI login when Git requests authentication; **never** place personal access tokens, passwords, SSH private keys or `.env` in the repo. Run `git status` and `git ls-files` first to inspect what will be uploaded. The `.gitignore` excludes `.env`, Parquet data and local logs. Add teammates through GitHub **Settings → Collaborators** after they agree.

## 3. Verify after pushing

Visit the newly created repository in your browser and verify that `README.md`, `jobs/`, `search_engine/`, `scripts/`, `tests/`, `docs/` and `.github/workflows/ci.yml` are present. Check the Actions tab for unit tests. **The GitHub unit CI does not prove the four-VM Spark/HDFS/Iceberg deployment works.** Follow `docs/verification_checklist.md` for full course acceptance.
