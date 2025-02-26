package io.bigconnect.dw.image.face;

import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents information about a processed image to prevent duplicate processing.
 * Thread-safe and immutable class that stores image metadata and handles comparison logic.
 */
public class ProcessedImageInfo {
    private static final int HASH_BUFFER_SIZE = 8192;
    private static final long DEFAULT_CACHE_TIMEOUT_MS = 5000; // 5 seconds
    private static final int MAX_TITLE_LENGTH = 1024; // Reasonable max length for title
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ProcessedImageInfo.class);
    @Getter
    private final String title;
    private final byte[] imageHash;
    private final long timestamp;
    private final long timeoutMs;
    private final String imageId; // Store element ID for additional verification

    /**
     * Creates a new ProcessedImageInfo instance
     *
     * @param title     The title of the image (can be null)
     * @param imageData The raw image data to hash
     * @param elementId The ID of the element being processed
     * @throws IllegalArgumentException if imageData is null or empty
     */
    public ProcessedImageInfo(String title, byte[] imageData, String elementId) {
        this(title, imageData, elementId, DEFAULT_CACHE_TIMEOUT_MS);
    }

    /**
     * Creates a new ProcessedImageInfo instance with custom timeout
     *
     * @param title     The title of the image (can be null)
     * @param imageData The raw image data to hash
     * @param elementId The ID of the element being processed
     * @param timeoutMs Custom timeout in milliseconds
     * @throws IllegalArgumentException if imageData is null or empty or timeout is invalid
     */
    public ProcessedImageInfo(String title, byte[] imageData, String elementId, long timeoutMs) {
        if (imageData == null || imageData.length == 0) {
            throw new IllegalArgumentException("Image data cannot be null or empty");
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        if (elementId == null || elementId.trim().isEmpty()) {
            throw new IllegalArgumentException("Element ID cannot be null or empty");
        }

        this.title = sanitizeTitle(title);
        this.imageHash = generateHash(imageData);
        this.timestamp = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
        this.imageId = elementId.trim();
    }

    /**
     * Sanitizes and truncates the title if necessary
     *
     * @param title The input title
     * @return Sanitized title
     */
    private String sanitizeTitle(String title) {
        if (title == null) {
            return "";
        }
        String sanitized = title.trim();
        return sanitized.length() > MAX_TITLE_LENGTH ?
                sanitized.substring(0, MAX_TITLE_LENGTH) : sanitized;
    }

    /**
     * Generates a SHA-256 hash of the image data using buffered reading
     *
     * @param data The image data to hash
     * @return The hash of the image data
     */
    private byte[] generateHash(byte[] data) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");

            // Process the data in chunks to handle large images efficiently
            for (int i = 0; i < data.length; i += HASH_BUFFER_SIZE) {
                int length = Math.min(HASH_BUFFER_SIZE, data.length - i);
                digest.update(data, i, length);
            }

            return digest.digest();
        } catch (Exception e) {
            LOGGER.error("Failed to generate hash for image", e);
            // Fallback to a simple hash if crypto fails
            return String.valueOf(Arrays.hashCode(data)).getBytes();
        }
    }

    /**
     * Checks if this processed image info has expired
     *
     * @return true if the cache entry has expired
     */
    public boolean isExpired() {
        return System.currentTimeMillis() - timestamp > timeoutMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessedImageInfo that = (ProcessedImageInfo) o;

        // Compare all relevant fields
        return Objects.equals(title, that.title) &&
                Arrays.equals(imageHash, that.imageHash) &&
                Objects.equals(imageId, that.imageId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(title, imageId);
        result = 31 * result + Arrays.hashCode(imageHash);
        return result;
    }

    @Override
    public String toString() {
        return String.format("ProcessedImageInfo{title='%s', imageId='%s', timestamp=%d, expired=%s}",
                title, imageId, timestamp, isExpired());
    }

    /**
     * Determines if this ProcessedImageInfo matches another considering all relevant factors
     *
     * @param other The other ProcessedImageInfo to compare with
     * @return true if the images should be considered the same for processing purposes
     */
    public boolean matches(ProcessedImageInfo other) {
        if (other == null) {
            return false;
        }

        // If either entry is expired, they don't match
        if (this.isExpired() || other.isExpired()) {
            return false;
        }

        // Compare all fields
        return this.equals(other) &&
                // Ensure timestamps are reasonably close (within timeout period)
                Math.abs(this.timestamp - other.timestamp) < timeoutMs;
    }

    /**
     * Creates a copy of this ProcessedImageInfo with a new timestamp
     *
     * @return A new ProcessedImageInfo with updated timestamp
     */
    public ProcessedImageInfo refresh() {
        return new ProcessedImageInfo(this.title, new byte[0], this.imageId, this.timeoutMs) {
            public byte[] generateHash(byte[] data) {
                return ProcessedImageInfo.this.imageHash;
            }
        };
    }
}
