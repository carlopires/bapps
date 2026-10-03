#!/bin/bash
# Sync local glommio to remote Linux server for testing

set -e

REMOTE="glommio-dev"
REMOTE_DIR="~/glommio"

echo "🔄 Syncing glommio to $REMOTE..."
rsync -avz --exclude 'target' --exclude '.git' --exclude '*.bak' \
    /Users/henrik.johansson/projects/pocs/glommio/ \
    $REMOTE:$REMOTE_DIR/

echo "✅ Sync complete!"
echo ""
echo "To run tests on remote:"
echo "  ssh $REMOTE 'cd $REMOTE_DIR && ./scripts/test-all-modules.sh'"
