#!/bin/bash
set -e
cd "$(dirname "$0")"

NPM_TOKEN="${1:?Usage: ./publish.sh <npm-token> <pypi-token>}"
PYPI_TOKEN="${2:?Usage: ./publish.sh <npm-token> <pypi-token>}"

echo "==> Publishing turbodatabase to npm..."
cd js
echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > .npmrc
npm publish --access public --skip-existing 2>/dev/null || echo "(already published or skipped)"
rm -f .npmrc
echo "✓ npm done"

echo "==> Publishing turbodatabase to PyPI..."
cd ../python
TWINE_USERNAME=__token__ TWINE_PASSWORD="$PYPI_TOKEN" /usr/bin/python3 -m twine upload dist/* --skip-existing
echo "✓ PyPI done"

echo "==> All published!"
