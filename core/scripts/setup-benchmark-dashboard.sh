#!/usr/bin/env bash
# Script to initialize GitHub Pages for benchmark dashboard
#
# Usage: ./scripts/setup-benchmark-dashboard.sh

set -euo pipefail

echo "🚀 Setting up benchmark dashboard..."
echo ""

# Save current branch
CURRENT_BRANCH=$(git branch --show-current)
echo "📍 Current branch: $CURRENT_BRANCH"

# Check if gh-pages already exists
if git rev-parse --verify gh-pages >/dev/null 2>&1; then
    echo "❌ Error: gh-pages branch already exists!"
    echo "   If you want to recreate it, first run:"
    echo "   git branch -D gh-pages"
    echo "   git push origin --delete gh-pages"
    exit 1
fi

echo "📝 Creating gh-pages branch..."
git checkout --orphan gh-pages

echo "🧹 Cleaning working directory..."
git rm -rf . >/dev/null 2>&1 || true

echo "📄 Creating index page..."
cat > index.html <<'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Glommio Benchmark Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            line-height: 1.6;
            padding: 2rem;
            max-width: 800px;
            margin: 0 auto;
            background: #f5f5f5;
        }
        .container {
            background: white;
            padding: 2rem;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            margin-bottom: 1rem;
        }
        p {
            color: #666;
            margin-bottom: 1.5rem;
        }
        .benchmarks {
            list-style: none;
        }
        .benchmarks li {
            margin: 0.5rem 0;
        }
        .benchmarks a {
            display: inline-block;
            padding: 0.75rem 1.5rem;
            background: #0066cc;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            transition: background 0.2s;
        }
        .benchmarks a:hover {
            background: #0052a3;
        }
        .info {
            margin-top: 2rem;
            padding: 1rem;
            background: #e7f3ff;
            border-left: 4px solid #0066cc;
            border-radius: 4px;
        }
        .info p {
            margin: 0;
            font-size: 0.9rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚡ Glommio Benchmark Dashboard</h1>
        <p>Continuous performance tracking for the Glommio async runtime.</p>

        <ul class="benchmarks">
            <li><a href="dev/bench/">📊 Timer Benchmarks</a></li>
        </ul>

        <div class="info">
            <p>
                <strong>Note:</strong> Benchmarks run on GitHub Actions shared runners.
                Results may show variance (±5-10%) due to shared infrastructure.
                Focus on trends rather than absolute values.
            </p>
        </div>
    </div>
</body>
</html>
EOF

echo "✅ Committing..."
git add index.html
git commit -m "Initialize benchmark dashboard"

echo "📤 Pushing to origin..."
git push origin gh-pages

echo "🔙 Returning to $CURRENT_BRANCH..."
git checkout "$CURRENT_BRANCH"

echo ""
echo "✨ Success! Benchmark dashboard initialized."
echo ""
echo "📋 Next steps:"
echo "   1. Enable GitHub Pages in repository settings:"
echo "      Settings → Pages → Source: gh-pages branch"
echo "   2. Wait a few minutes for deployment"
echo "   3. View at: https://<username>.github.io/<repo>/"
echo ""
echo "💡 After the first benchmark run on master, charts will appear at:"
echo "   https://<username>.github.io/<repo>/dev/bench/"
