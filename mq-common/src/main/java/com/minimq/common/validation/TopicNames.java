package com.minimq.common.validation;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Locale;
import java.util.Set;

/** Portable topic-name rules shared by protocol, metadata, and storage boundaries. */
public final class TopicNames {
    public static final int MAX_LENGTH = 249;
    private static final Set<String> WINDOWS_RESERVED_NAMES = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private TopicNames() {
    }

    public static String requireValid(String topicName) {
        if (!isValid(topicName)) {
            throw new IllegalArgumentException(
                    "topicName must be 1.." + MAX_LENGTH
                            + " portable ASCII letters, digits, '.', '_', or '-'"
                            + " without reserved names or trailing dots");
        }
        return topicName;
    }

    public static boolean isValid(String topicName) {
        if (topicName == null
                || topicName.isEmpty()
                || topicName.length() > MAX_LENGTH
                || ".".equals(topicName)
                || "..".equals(topicName)) {
            return false;
        }
        for (int index = 0; index < topicName.length(); index++) {
            char value = topicName.charAt(index);
            boolean allowed = value >= 'a' && value <= 'z'
                    || value >= 'A' && value <= 'Z'
                    || value >= '0' && value <= '9'
                    || value == '.'
                    || value == '_'
                    || value == '-';
            if (!allowed) {
                return false;
            }
        }
        return isValidReference(topicName);
    }

    /**
     * Accepts an existing pre-V4 topic when it is still one portable path
     * component. New topic creation must continue to use {@link #isValid}.
     */
    public static String requireValidReference(String topicName) {
        if (!isValidReference(topicName)) {
            throw new IllegalArgumentException(
                    "topicName is not a portable existing topic reference");
        }
        return topicName;
    }

    public static boolean isValidReference(String topicName) {
        if (topicName == null
                || topicName.isBlank()
                || topicName.length() > MAX_LENGTH
                || topicName.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH
                || ".".equals(topicName)
                || "..".equals(topicName)
                || topicName.endsWith(".")
                || topicName.endsWith(" ")) {
            return false;
        }
        for (int index = 0; index < topicName.length(); index++) {
            char value = topicName.charAt(index);
            if (value < 0x20 || value == 0x7f
                    || value == '/' || value == '\\' || value == ':'
                    || value == '*' || value == '?' || value == '"'
                    || value == '<' || value == '>' || value == '|') {
                return false;
            }
        }
        String firstComponent = topicName.split("\\.", 2)[0].toUpperCase(Locale.ROOT);
        return !WINDOWS_RESERVED_NAMES.contains(firstComponent);
    }

    /** Identity used to prevent aliases on case-insensitive or normalizing filesystems. */
    public static String portableStorageIdentity(String topicName) {
        requireValidReference(topicName);
        return Normalizer.normalize(topicName, Normalizer.Form.NFC).toLowerCase(Locale.ROOT);
    }
}
