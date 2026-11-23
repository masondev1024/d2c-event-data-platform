FROM python:3.13-slim-trixie@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285

# The integration runtime executes the real writer's application and migration.
# PyMySQL's RSA extra supports the isolated MySQL 8 server's default auth plugin.
COPY --from=gitops_writer requirements.txt /tmp/writer-requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/writer-requirements.txt 'PyMySQL[rsa]' kafka-python==3.0.11
COPY --from=gitops_writer . /writer
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /writer
USER 65532:65532
