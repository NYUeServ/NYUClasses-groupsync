package edu.nyu.classes.groupsync.main.brightspace;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSON {

    private Object elt;
    private boolean missing;
    private String failedQuery;

    public static JSON parse(String json) {
        try {
            Object result = new JSONParser().parse(json);
            return new JSON(result);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static JSON parse(byte[] json) {
        return JSON.parse(new String(json, java.nio.charset.StandardCharsets.UTF_8));
    }

    public static JSON parse(Reader stream) {
        try {
            Object result = new JSONParser().parse(stream);
            return new JSON(result);
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public JSON(Object parsed) {
        elt = parsed;
        missing = false;
    }

    public JSON(Object failedObj, boolean missing, String failedQuery) {
        elt = failedObj;
        this.missing = missing;
        this.failedQuery = failedQuery;
    }


    public String toString() {
        return elt.toString();
    }

    public JSON path(String query) {
        String[] bits = query.split(" *> *");
        Object target = elt;

        for (String bit : bits) {
            if (bit.matches("\\[[0-9]+\\]")) {
                // Array index
                checkType(target, JSONArray.class);

                int idx = Integer.valueOf(bit.substring(1, bit.length() - 1));

                if (idx >= ((JSONArray)target).size()) {
                    return new JSON(elt, true, query);
                } else {
                    target = ((JSONArray)target).get(idx);
                }
            } else {
                // Key lookup
                checkType(target, JSONObject.class);

                if (((JSONObject)target).containsKey(bit)) {
                    target = ((JSONObject)target).get(bit);
                } else {
                    return new JSON(elt, true, query);
                }
            }
        }

        return new JSON(target);
    }

    private void checkPresent() {
        if (this.missing) {
            throw new RuntimeException(String.format("No values were matched for query '%s' against: %s",
                                                     this.failedQuery,
                                                     this.elt));
        }
    }

    public Long asLong(Long dflt) {
        if (this.missing || this.elt == null) {
            return dflt;
        } else {
            return asLongOrDie();
        }
    }

    public Long asLongOrDie() {
        checkPresent();

        if (elt instanceof Integer) {
            return Long.valueOf((Integer)elt);
        } else if (elt instanceof Long) {
            return (Long)elt;
        }

        throw new RuntimeException(String.format("Expected a long but had: %s", String.valueOf(elt)));
    }

    public String asString(String dflt) {
        if (this.missing || this.elt == null) {
            return dflt;
        } else {
            return asStringOrDie();
        }
    }

    public String asStringOrDie() {
        checkPresent();

        if (elt instanceof String) {
            return (String)elt;
        }

        throw new RuntimeException(String.format("Expected a string but had: %s", String.valueOf(elt)));
    }

    public Boolean asBoolean(Boolean dflt) {
        if (this.missing || this.elt == null) {
            return dflt;
        } else {
            return asBooleanOrDie();
        }
    }

    public Boolean asBooleanOrDie() {
        checkPresent();

        if (elt instanceof Boolean) {
            return (Boolean)elt;
        }

        throw new RuntimeException(String.format("Expected a boolean but had: %s", String.valueOf(elt)));
    }

    public boolean isMissing() {
        return missing;
    }

    public boolean isPresent() {
        return !missing;
    }

    public boolean isNull() {
        checkPresent();

        return elt == null;
    }

    public boolean isEmpty() {
        checkPresent();

        if (elt instanceof JSONObject) {
            return ((JSONObject) elt).size() == 0;
        } else if (elt instanceof JSONArray) {
            return ((JSONArray) elt).size() == 0;
        } else if (elt instanceof String) {
            return ((String) elt).length() == 0;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public List<JSON> asJSONList() {
        checkPresent();
        checkType(elt, JSONArray.class);

        List<JSON> result = new ArrayList<>();

        ((JSONArray)elt).stream().forEach((obj) -> result.add(new JSON(obj)));

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<String> asStringList() {
        checkPresent();
        checkType(elt, JSONArray.class);

        List<String> result = new ArrayList<>();
        ((JSONArray)elt).stream().forEach((obj) -> result.add((String)obj));
        return result;
    }

    @SuppressWarnings("unchecked")
    public List<String> keys() {
        checkPresent();
        checkType(elt, JSONObject.class);

        return new ArrayList<String>(((JSONObject)elt).keySet());
    }

    @SuppressWarnings("unchecked")
    public List<Long> asLongList() {
        checkPresent();
        checkType(elt, JSONArray.class);

        List<Long> result = new ArrayList<>();
        ((JSONArray)elt).stream().forEach((obj) -> result.add((Long)obj));
        return result;
    }


    private void checkType(Object obj, Class<?> clz) {
        if (obj.getClass().equals(clz)) {
            return;
        }

        throw new RuntimeException(String.format("JSON element %s was not of type %s",
                                                 obj,
                                                 clz.getName()));
    }
}
