#!/bin/bash

# Configuration
# Run the script in the directory where your .zip files are, or pass the path as an argument.
SOURCE_DIR="${1:-.}"
TARGET_BASE="/lobster"

echo "📂 Scanning for LOBSTER zip files in: $SOURCE_DIR"

# Loop through all .zip files in the source directory
for zip_file in "$SOURCE_DIR"/*.zip; do
    # Skip if no zip files are found
    [ -e "$zip_file" ] || continue

    echo "========================================"
    echo "📦 Extracting: $(basename "$zip_file")"

    # Create a safe, temporary extraction directory
    temp_dir=$(mktemp -d)

    # Extract the .zip into the temp directory quietly
    unzip -q "$zip_file" -d "$temp_dir"

    # Iterate over the extracted .7z files
    for archive in "$temp_dir"/*.7z; do
        # Skip if no .7z files are found inside
        [ -e "$archive" ] || continue

        filename=$(basename "$archive")

        # Parse the filename using regex: <TICKER>_<YYYY>_<MM>_<DD>.7z
        if [[ "$filename" =~ ^([A-Za-z0-9]+)_([0-9]{4})_([0-9]{2})_([0-9]{2})\.7z$ ]]; then
            ticker="${BASH_REMATCH[1]}"
            yyyy="${BASH_REMATCH[2]}"
            mm="${BASH_REMATCH[3]}"
            dd="${BASH_REMATCH[4]}"

            # Construct target paths
            target_dir="$TARGET_BASE/$yyyy/${yyyy}_${mm}_${dd}"
            target_path="$target_dir/${ticker}.7z"

            # Create the nested directory structure if it doesn't exist
            mkdir -p "$target_dir"

            # Move the .7z file to its final destination
            mv "$archive" "$target_path"
            echo "   ✅ Moved: $ticker to $target_dir/"
        else
            echo "   ⚠️ Skipped: $filename (Filename did not match expected pattern)"
        fi
    done

    # Clean up the temporary directory
    rm -rf "$temp_dir"

    # UNCOMMENT the line below if you want to delete the original .zip after organizing
    # rm "$zip_file"
done

echo "========================================"
echo "🎉 All files organized successfully into $TARGET_BASE!"