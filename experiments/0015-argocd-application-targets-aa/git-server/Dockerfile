# Tiny smart-HTTP git server for experiment 0015.
#
# debian:slim's git package ships git-daemon AND git-http-backend, plus
# we add apache2 with mod_cgi to invoke git-http-backend for the smart
# HTTP protocol. ArgoCD's go-git client wants smart HTTP (shallow
# clones); dumb HTTP via static files does not suffice.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      git git-daemon-run apache2 \
 && rm -rf /var/lib/apt/lists/*

# Enable the CGI apache modules required by git-http-backend.
RUN a2enmod cgi alias env rewrite \
 && a2dissite 000-default

# Apache site config: map /aggexp.git/* to git-http-backend CGI.
# GIT_HTTP_EXPORT_ALL makes every repo under /srv/git served without
# needing a daemon-export-ok marker.
COPY apache-git.conf /etc/apache2/sites-available/git.conf
RUN a2ensite git

# Seed script runs at container start to (re)build the bare repo from
# /content (ConfigMap mount) before apache is started.
COPY seed.sh /usr/local/bin/seed.sh
RUN chmod +x /usr/local/bin/seed.sh

# Apache in foreground
ENV APACHE_RUN_USER=www-data
ENV APACHE_RUN_GROUP=www-data
ENV APACHE_LOG_DIR=/var/log/apache2
ENV APACHE_PID_FILE=/var/run/apache2/apache2.pid
ENV APACHE_RUN_DIR=/var/run/apache2
ENV APACHE_LOCK_DIR=/var/lock/apache2

EXPOSE 8080
CMD ["/bin/sh", "-ec", "/usr/local/bin/seed.sh && exec apache2 -D FOREGROUND"]
