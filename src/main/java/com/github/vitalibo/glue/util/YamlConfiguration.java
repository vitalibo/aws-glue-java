package com.github.vitalibo.glue.util;

import com.amazonaws.services.glue.util.JsonOptions;
import com.amazonaws.util.json.Jackson;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.yaml.snakeyaml.Yaml;

import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class YamlConfiguration {

    private final static Yaml yaml = new Yaml();

    private final Map<String, Object> objectMap;

    public static YamlConfiguration parseResources(String resource) {
        return new YamlConfiguration(
            yaml.load(
                YamlConfiguration.class.getResourceAsStream(
                    resource)));
    }

    public YamlConfiguration withFallback(YamlConfiguration other) {
        @AllArgsConstructor
        class Wrap {
            Map<String, Object> items;

            Map<String, Object> merge(Map<String, Object> other) {
                for (Map.Entry<String, Object> entry : other.entrySet()) {
                    Object value = entry.getValue();
                    if (value instanceof Map) {
                        value = new Wrap((Map<String, Object>) items.getOrDefault(entry.getKey(), new HashMap<>()))
                            .merge((Map<String, Object>) entry.getValue());
                    }

                    items.put(entry.getKey(), value);
                }

                return items;
            }
        }

        return new YamlConfiguration(
            new Wrap(objectMap)
                .merge(other.objectMap));
    }

    public Optional<Object> get(String path) {
        Object obj = objectMap;
        for (String key : path.split("\\.")) {
            obj = ((Map<String, Object>) obj).get(key);
        }

        return Optional.ofNullable(obj);
    }

    public YamlConfiguration asObject(String path) {
        return get(path)
            .map(o -> (Map<String, Object>) o)
            .map(YamlConfiguration::new)
            .orElseGet(() -> new YamlConfiguration(new HashMap<>()));
    }

    public List<YamlConfiguration> asListObjects(String path) {
        return get(path)
            .map(o -> (List<Map<String, Object>>) o)
            .map(o -> o.stream()
                .map(YamlConfiguration::new)
                .collect(Collectors.toList()))
            .orElseGet(ArrayList::new);
    }

    public JsonOptions getJsonOptions(String path) {
        return get(path)
            .map(Jackson::toJsonString)
            .map(JsonOptions::new)
            .orElseGet(JsonOptions::empty);
    }

    public String getString(String path) {
        return getString(path, null);
    }

    public String getString(String path, String other) {
        return get(path)
            .map(o -> (String) o)
            .orElse(other);
    }

    public Integer getInteger(String path) {
        return getInteger(path, null);
    }

    public Integer getInteger(String path, Integer other) {
        return get(path)
            .map(o -> (Integer) o)
            .orElse(other);
    }

    public Double getDouble(String path) {
        return getDouble(path, null);
    }

    public Double getDouble(String path, Double other) {
        return get(path)
            .map(o -> (Double) o)
            .orElse(other);
    }

}
