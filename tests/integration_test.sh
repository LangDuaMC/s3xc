#!/bin/bash
set -eo pipefail

echo "===================================================="
echo "🚀 Starting Integration Tests for s3xc Caching Proxy"
echo "===================================================="

# Helper function to wait for a port to open
wait_for_port() {
  local name=$1
  local host=$2
  local port=$3
  echo "⌛ Waiting for $name to be ready on $host:$port..."
  for i in {1..30}; do
    if nc -z "$host" "$port"; then
      echo "✅ $name is ready!"
      return 0
    fi
    sleep 1
  done
  echo "❌ Timeout waiting for $name"
  return 1
}

# Wait for MinIO and s3xc
wait_for_port "MinIO" "localhost" 9000
wait_for_port "s3xc Proxy" "localhost" 8080

# Configure mc client aliases
echo "🔧 Configuring MinIO mc aliases..."
mc alias set my-minio http://localhost:9000 admin password

mc alias set my-s3xc http://localhost:8080 proxy_user proxy_password

# 1. Create a bucket in MinIO
echo "🪣 Creating bucket 'test-bucket' in MinIO..."
mc mb --ignore-existing my-minio/test-bucket

# Verify bucket listing is proxied via s3xc (fallback test)
echo "🔍 Listing buckets via s3xc (testing fallback reverse proxy)..."
aws --endpoint-url http://localhost:8080 s3 ls

# 2. Upload a file via s3xc
echo "📤 Generating 5MB test file..."
dd if=/dev/urandom of=test_5mb.bin bs=1M count=5

echo "📤 Uploading test_5mb.bin to s3xc (should write cache chunks)..."
aws --endpoint-url http://localhost:8080 s3 cp test_5mb.bin s3://test-bucket/test_5mb.bin

# 3. Verify file exists in MinIO directly (proves write-through proxy works)
echo "🔍 Checking file directly in MinIO..."
mc ls my-minio/test-bucket/test_5mb.bin

# Helper to get custom cache header using AWS CLI debug output
get_cache_header() {
  local bucket=$1
  local key=$2
  aws --endpoint-url http://localhost:8080 s3api head-object --bucket "$bucket" --key "$key" --debug 2>&1 | sed -n "s/.*'x-s3xc-cached': '\([^']*\)'.*/\1/p" | tr '[:upper:]' '[:lower:]'
}

# 4. Fetch the file via s3xc
echo "📥 Fetching file from s3xc (should hit cache)..."
aws --endpoint-url http://localhost:8080 s3 cp s3://test-bucket/test_5mb.bin retrieved_cache_hit.bin
diff test_5mb.bin retrieved_cache_hit.bin
echo "✅ Cache hit verify: retrieved data matches original"

# 5. Range Request (GET range) from s3xc
echo "📥 Fetching specific byte range (bytes=1048576-2097151) from s3xc..."
aws --endpoint-url http://localhost:8080 s3api get-object \
  --bucket test-bucket \
  --key test_5mb.bin \
  --range bytes=1048576-2097151 \
  retrieved_range.bin

# Check size of range (should be exactly 1MB)
RANGE_SIZE=$(wc -c < retrieved_range.bin)
if [ "$RANGE_SIZE" -eq 1048576 ]; then
  echo "✅ Range request size matches: 1MB"
else
  echo "❌ Error: Expected 1MB range size, got $RANGE_SIZE bytes"
  exit 1
fi

# Compare bytes of range with original
dd if=test_5mb.bin of=expected_range.bin bs=1 skip=1048576 count=1048576 status=none
diff expected_range.bin retrieved_range.bin
echo "✅ Range data matches exactly!"

# 6. Copy Test (cp)
echo "📋 Copying test_5mb.bin to test_5mb_copy.bin via s3xc..."
aws --endpoint-url http://localhost:8080 s3 cp s3://test-bucket/test_5mb.bin s3://test-bucket/test_5mb_copy.bin

echo "🔍 Checking test_5mb_copy.bin exists upstream..."
mc ls my-minio/test-bucket/test_5mb_copy.bin

echo "🔍 Verifying cache status of the copy (should be hot, cache is copied from source)..."
# The proxy copies local cache chunks from source to destination,
# so the copy should be hot immediately.
CACHE_HEADER_CP=$(get_cache_header test-bucket test_5mb_copy.bin)
echo "Cache Header for copy: $CACHE_HEADER_CP"
if [[ "$CACHE_HEADER_CP" == "hot" ]]; then
  echo "✅ Copy cache status matches: hot"
else
  echo "❌ Error: Expected hot cache status for copy, got '$CACHE_HEADER_CP'"
  exit 1
fi

# 7. Move Test (mv)
echo "📦 Moving test_5mb.bin to test_5mb_moved.bin via s3xc..."
aws --endpoint-url http://localhost:8080 s3 mv s3://test-bucket/test_5mb.bin s3://test-bucket/test_5mb_moved.bin

echo "🔍 Checking test_5mb_moved.bin exists upstream..."
mc ls my-minio/test-bucket/test_5mb_moved.bin

echo "📋 DEBUG: All objects in bucket after mv:"
mc ls my-minio/test-bucket/

# Debug: explicit delete via s3api through the proxy with debug output
echo "🔍 Explicit delete of test_5mb.bin via s3xc proxy..."
aws --endpoint-url http://localhost:8080 s3api delete-object --bucket test-bucket --key test_5mb.bin --debug 2>&1 | grep -i -E '(http|status|delete|error|response)' || true

echo "📋 DEBUG: All objects in bucket after explicit proxy delete:"
mc ls my-minio/test-bucket/

# If still exists, try direct MinIO delete as control test
if mc stat my-minio/test-bucket/test_5mb.bin 2>/dev/null; then
  echo "⚠️ Still exists after proxy delete. Trying direct MinIO delete..."
  mc rm my-minio/test-bucket/test_5mb.bin
  echo "📋 DEBUG: All objects after direct MinIO delete:"
  mc ls my-minio/test-bucket/
  echo "❌ Error: Proxy DELETE did not remove object from upstream MinIO!"
  exit 1
fi

echo "🔍 Checking cache status of moved file (should be hot)..."
CACHE_HEADER_MV=$(get_cache_header test-bucket test_5mb_moved.bin)
echo "Cache Header for moved: $CACHE_HEADER_MV"
if [[ "$CACHE_HEADER_MV" == "hot" ]]; then
  echo "✅ Move cache status matches: hot"
else
  echo "❌ Error: Expected hot cache status for moved file, got '$CACHE_HEADER_MV'"
  exit 1
fi

# 8. Delete Test (rm)
echo "🗑️ Deleting test_5mb_moved.bin via s3xc..."
aws --endpoint-url http://localhost:8080 s3 rm s3://test-bucket/test_5mb_moved.bin

echo "🔍 Checking test_5mb_moved.bin does not exist upstream..."
if mc stat my-minio/test-bucket/test_5mb_moved.bin 2>/dev/null; then
  echo "❌ Error: Deleted file still exists upstream!"
  exit 1
fi

# 9. Test Multiple Credentials (apple and blossom)
echo "🔑 Testing multiple credentials..."
mc alias set my-s3xc-apple http://localhost:8080 apple applepieiskey
mc alias set my-s3xc-blossom http://localhost:8080 blossom blossomcakeiskey

echo "🔍 Listing buckets via apple credential..."
mc ls my-s3xc-apple

echo "🔍 Listing buckets via blossom credential..."
mc ls my-s3xc-blossom

# Clean up aliases
mc alias rm my-s3xc-apple
mc alias rm my-s3xc-blossom

# Clean up local test files
rm -f test_5mb.bin retrieved_cache_hit.bin retrieved_range.bin expected_range.bin

echo "===================================================="
echo "🎉 All Integration Tests Passed!"
echo "===================================================="
