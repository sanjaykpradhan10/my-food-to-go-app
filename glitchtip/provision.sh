#!/bin/sh
set -eu

# Bootstraps a superuser, organization, and project via GlitchTip's Django management shell
# (more reliable than guessing REST payload shapes for a first-run, unauthenticated instance),
# then reads the project's DSN straight off the created ProjectKey and writes it where the app
# containers can pick it up.
#
# Model import paths and field names below were verified live against glitchtip/glitchtip:v4.2.9
# (see task-1-report.md) — they differ from what a naive reading of GlitchTip's docs would
# suggest: models live under `apps.<app>.models`, not `<app>.models`, and the user model has no
# `username` field (email is the identifier; the display-name field is `name`).
#
# This container has no `docker compose` CLI (docker:27-cli ships only the docker client), so
# it targets the glitchtip container by its compose-assigned label rather than `compose exec`.

GLITCHTIP_CID=$(docker ps -q -f label=com.docker.compose.service=glitchtip)

docker exec "$GLITCHTIP_CID" python manage.py shell -c "
from django.contrib.auth import get_user_model
from apps.organizations_ext.models import Organization
from apps.projects.models import Project, ProjectKey

User = get_user_model()
user, _ = User.objects.get_or_create(email='admin@localhost', defaults={'name': 'admin', 'is_superuser': True, 'is_staff': True})
user.set_password('admin')
user.save()

org, _ = Organization.objects.get_or_create(name='ftgo')
if not org.is_member(user):
    org.add_user(user)

project, _ = Project.objects.get_or_create(name='ftgo', organization=org)

key = ProjectKey.objects.filter(project=project).first()
if key is None:
    key = ProjectKey.objects.create(project=project)

print('DSN=' + key.get_dsn())
" > /tmp/glitchtip-shell-output.txt

DSN_LINE=$(grep '^DSN=' /tmp/glitchtip-shell-output.txt)
echo "SENTRY_${DSN_LINE}" > dsn.env
cat dsn.env
