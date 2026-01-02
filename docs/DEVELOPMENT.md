# Content: apt prerequisites
# Node install method (nvm recommended)
# how to run backend and frontend

# Development setup (Debian/Ubuntu)

## System prerequisites

```bash
sudo apt-get update
sudo apt-get install -y \
  git \
  curl \
  build-essential



Link this from your `README.md`.


## 6) Optional but very clean: add a bootstrap script

Create `scripts/bootstrap-debian.sh`:

```bash
#!/usr/bin/env bash
set -e

sudo apt-get update
sudo apt-get install -y git curl build-essential

if ! command -v nvm >/dev/null; then
  echo "Installing nvm..."
  curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
fi

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"

nvm install 24
nvm use 24


##  Install npm 

```bash
cd geomap-ui
npm install
npm run dev