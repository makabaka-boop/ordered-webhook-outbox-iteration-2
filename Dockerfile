# Pure standard library -> no build-time network access needed.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/srv:/srv/app

WORKDIR /srv

# Shared image for api / worker / fake-target / verify.
COPY app/ /srv/app/
COPY verify/ /srv/verify/

RUN mkdir -p /data && chown -R nobody:nogroup /data /srv
USER nobody

# default; each service overrides the command
CMD ["python", "-m", "app.api"]
