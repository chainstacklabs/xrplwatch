#!/usr/bin/env bash
# Check that everything still works.
#
#   ./smoke.sh              tests plus the network checks
#   ./smoke.sh --offline    tests only, no network
set -uo pipefail
cd "$(dirname "$0")"

worked=0
broke=0

pass() { echo "$1"; ((worked++)); }
fail() { echo "BROKEN${1:+ ($1)}"; ((broke++)); }

printf '%-30s ' "offline tests"
if out=$(uv run --quiet pytest 2>&1); then
  pass "$(echo "$out" | tail -1)"
else
  fail "$(echo "$out" | tail -3 | tr '\n' ' ')"
fi

if [[ "${1:-}" == "--offline" ]]; then
  echo
  echo "worked=$worked broken=$broke (network checks skipped)"
  exit $(( broke > 0 ? 1 : 0 ))
fi

printf '%-30s ' "finding servers"
if out=$(uv run --quiet python -c "
from xrplwatch import discovery
found = discovery.find_servers(20)
assert len(found) > 5, f'only found {len(found)} servers'
print(f'{len(found)} listed')
" 2>&1); then pass "$out"; else fail "$out"; fi

printf '%-30s ' "connecting to a server"
if out=$(uv run --quiet python -c "
from xrplwatch import connection, discovery, identity
for address, port in discovery.find_servers(25):
    try:
        peer = connection.open_connection(address, port, identity.create_identity())
    except Exception:
        continue
    peer.link.close()
    print(f'{address}:{port} agreed {peer.style}')
    raise SystemExit(0)
raise SystemExit('no server accepted a connection')
" 2>&1); then pass "$out"; else fail "$out"; fi

printf '%-30s ' "reading transactions"
# The feed runs until stopped, so being stopped is expected. What matters is
# whether it printed transactions before that.
printed=$(timeout 90 uv run --quiet python xrpl_feed.py --servers 8 --watch trades --quiet 2>/dev/null \
          | grep -c '"TransactionType"' || true)
if (( printed > 0 )); then
  pass "$printed transactions"
else
  fail "nothing printed in 90 seconds"
fi

echo
echo "worked=$worked broken=$broke"
exit $(( broke > 0 ? 1 : 0 ))
