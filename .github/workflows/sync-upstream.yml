name: Sync Upstream (Auto-update forks)

on:
  # هر push به main → upstream را آپدیت می‌کند
  push:
    branches: [main]
  # هر ۶ ساعت یک بار چک می‌کند
  schedule:
    - cron: '0 */6 * * *'
  workflow_dispatch:

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Push to forks (notify GitHub)
        run: |
          echo "✅ Upstream updated at $(date)"
          echo "Forks should use GitHub's 'Sync fork' button or enable auto-sync."
