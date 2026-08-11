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
#
# key.get_dsn() bakes in GLITCHTIP_DOMAIN (http://localhost:8000), which is correct for a human
# hitting the UI from the host machine but wrong for the DSN handed to the *other* 9 service
# containers (Task 2) — from inside those containers "localhost" resolves to themselves, not to
# glitchtip. Rewrite the DSN's host to the compose service name "glitchtip", which every
# container on this compose network can resolve, without touching GLITCHTIP_DOMAIN itself.
#
# Also mints a GlitchTip internal API token (Task 3, Ch.11 §11.3.5 verification) scoped to
# org:read/project:read/event:read/member:read — the minimum the e2e test's issues-API polling
# needs. GlitchTip's `scopes` field is a django-bitfield: passing scopes=[...] to the model
# constructor silently no-ops (it coerces to an int, not the bit list), so each flag must be set
# individually via setattr(token.scopes, '<flag>', True) after creation, then saved. The token is
# get_or_create'd by label so re-running provisioning (e.g. a compose restart) doesn't mint a new
# one, keep old ones live, or need a revocation step of its own.

GLITCHTIP_CID=$(docker ps -q -f label=com.docker.compose.service=glitchtip)

docker exec "$GLITCHTIP_CID" python manage.py shell -c "
from django.contrib.auth import get_user_model
from apps.organizations_ext.models import Organization
from apps.projects.models import Project, ProjectKey
from apps.api_tokens.models import APIToken

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

token = APIToken.objects.filter(user=user, label='e2e-test').first()
if token is None:
    token = APIToken.objects.create(user=user, label='e2e-test')
    for flag in ('org:read', 'project:read', 'event:read', 'member:read'):
        setattr(token.scopes, flag, True)
    token.save()

print('APITOKEN=' + token.token)
" > /tmp/glitchtip-shell-output.txt

DSN_LINE=$(grep '^DSN=' /tmp/glitchtip-shell-output.txt)
REWRITTEN_DSN_LINE=$(echo "$DSN_LINE" | sed 's/@localhost:8000/@glitchtip:8000/')
# The sed above is an exact-substring rewrite, not a general URL-host rewrite: if
# GLITCHTIP_DOMAIN (compose.yml) is ever anything other than exactly "http://localhost:8000",
# the pattern silently fails to match and sed exits 0 with the DSN unchanged — producing a DSN
# that bakes in a host none of the other 9 containers can resolve, with no error anywhere in
# this script's output. Fail loudly instead of shipping an unreachable DSN.
if [ "$REWRITTEN_DSN_LINE" = "$DSN_LINE" ]; then
  echo "provision.sh: expected DSN host rewrite (@localhost:8000 -> @glitchtip:8000) to change" >&2
  echo "the DSN, but it didn't — GLITCHTIP_DOMAIN in compose.yml no longer matches this script's" >&2
  echo "hardcoded assumption. Update the sed pattern above to match the new domain." >&2
  exit 1
fi
DSN_LINE="$REWRITTEN_DSN_LINE"
TOKEN_LINE=$(grep '^APITOKEN=' /tmp/glitchtip-shell-output.txt)

{
  echo "SENTRY_${DSN_LINE}"
  echo "GLITCHTIP_API_TOKEN=${TOKEN_LINE#APITOKEN=}"
} > dsn.env
cat dsn.env
