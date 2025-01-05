#!/bin/bash

# Variables
VERSION="1.9.0"
OUTPUT_FILE="liftbridge"

# OS and architecture combinations
OS_ARCH_COMBINATIONS=(
  "linux:amd64"
  "linux:arm64"
  "darwin:amd64"
  "darwin:arm64"
)
mkdir -p releases

# Loop through combinations
for COMBO in "${OS_ARCH_COMBINATIONS[@]}"; do
  # Split OS and ARCH
  IFS=":" read -r GOOS GOARCH <<< "$COMBO"
  
  # Set output filename and tarball name
  BIN_FILE="$OUTPUT_FILE"
  [ "$GOOS" = "windows" ] && BIN_FILE="${OUTPUT_FILE}.exe"
  TAR_FILE="${OUTPUT_FILE}_${VERSION}_termux_${GOOS}_${GOARCH}.tar.gz"
  
  # Build the binary
  echo "Building for GOOS=${GOOS}, GOARCH=${GOARCH}..."
  env GOOS=$GOOS GOARCH=$GOARCH go build -o "$BIN_FILE"
  if [ $? -ne 0 ]; then
    echo "Build failed for GOOS=${GOOS}, GOARCH=${GOARCH}!"
    exit 1
  fi
  echo "Build successful: $BIN_FILE"

  # Compress the binary into a tarball
  echo "Compressing into $TAR_FILE..."
  tar -czf "$TAR_FILE" "$BIN_FILE"
  if [ $? -ne 0 ]; then
    echo "Compression failed for $BIN_FILE!"
    exit 1
  fi
  echo "Compression successful: $TAR_FILE"

  # Clean up the binary
  echo "Cleaning up $BIN_FILE..."
  rm "$BIN_FILE"
done
mv *.tar.gz ./releases/
echo "All builds and compressions completed!"