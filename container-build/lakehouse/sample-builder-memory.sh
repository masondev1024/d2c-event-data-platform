# Executed by sh inside the inspected BuildKit container. With a private cgroup
# namespace its root is the container. In a host namespace the executor can be
# in a child cgroup; the budget applies at the owning container's ancestor.
set -eu
mount=$1
membership=$2
cid=$3
limit=$4
namespace=$5
relative=$(awk -F: '$1 == "0" {print $3}' "$membership")
case "$relative" in /*) ;; *) echo "missing unified cgroup membership" >&2; exit 1 ;; esac
dir="${mount}${relative%/}"
while [ "$dir" != "$mount" ]; do
    case ${dir##*/} in "$cid"|"docker-$cid.scope") break ;; esac
    dir=${dir%/*}
done
if [ "$dir" = "$mount" ] && [ "$namespace" != private ]; then
    echo "container scope absent in $namespace namespace (membership $relative)" >&2
    exit 1
fi
cd "$dir"
actual=$(cat memory.max)
if [ "$actual" != "$limit" ]; then
    echo "cgroup memory.max=$actual differs from builder limit=$limit (membership $relative)" >&2
    exit 1
fi
awk 'FILENAME=="memory.current" {print "current_bytes", $1}
     FILENAME=="memory.stat" && ($1=="anon" || $1=="file") {print $1 "_bytes", $2}
     FILENAME=="memory.events" && ($1=="oom" || $1=="oom_kill") {print $1, $2}' \
    memory.current memory.stat memory.events
if [ -r memory.peak ]; then printf 'peak_bytes %s\n' "$(cat memory.peak)"; fi
if [ -r memory.swap.current ]; then printf 'swap_bytes %s\n' "$(cat memory.swap.current)"; fi
