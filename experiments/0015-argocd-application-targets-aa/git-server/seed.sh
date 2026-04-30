#!/bin/sh
# Rebuild the bare repo at /srv/git/aggexp.git from /content/*.yaml.
# Invoked at container start. Idempotent.
set -eux
rm -rf /srv/git/work /srv/git/aggexp.git
mkdir -p /srv/git /srv/git/work
cp /content/*.yaml /srv/git/work/
cd /srv/git/work
git init -q -b main
git config user.email "git-server@aggexp.local"
git config user.name  "git-server"
git add .
git commit -q -m "aggexp Widget manifests from ConfigMap"
cd /srv/git
git clone --bare work aggexp.git
rm -rf work
cd aggexp.git
# Smart HTTP needs the receive-pack/upload-pack configs (defaults ok
# for upload-pack / clone, which is all argocd issues).
chown -R www-data:www-data /srv/git
ls -la /srv/git/aggexp.git
