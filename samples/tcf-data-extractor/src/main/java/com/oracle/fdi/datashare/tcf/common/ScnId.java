package com.oracle.fdi.datashare.tcf.common;

public final class ScnId {

    private final long value;

    private ScnId(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("SCN must be non-negative");
        }
        this.value = value;
    }

    public static ScnId fromChangeId(String changeId) {
        if (changeId == null || changeId.isBlank()) {
            throw new IllegalArgumentException("changeId must not be blank");
        }

        String scnStr;
        if (changeId.startsWith("fdi_scn_id=")) {
            scnStr = changeId.substring("fdi_scn_id=".length());
        } else {
            scnStr = changeId;
        }

        return parseLongStrict(scnStr, changeId);
    }

    public static ScnId fromMarker(String markerScn) {
        if (markerScn == null || markerScn.isBlank()) {
            throw new IllegalArgumentException("markerScn must not be blank");
        }
        return parseLongStrict(markerScn, markerScn);
    }

    private static ScnId parseLongStrict(String scnStr, String original) {
        try {
            return new ScnId(Long.parseLong(scnStr));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid SCN value: '" + original + "'", e);
        }
    }

    public long value() {
        return value;
    }

}
