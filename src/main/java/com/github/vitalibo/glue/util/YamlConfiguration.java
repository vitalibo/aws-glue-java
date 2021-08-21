package com.github.vitalibo.glue.util;

import com.amazonaws.services.glue.util.JsonOptions;
import com.amazonaws.util.json.Jackson;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public Object get(String path) {
        Object obj = objectMap;
        for (String key : path.split("\\.")) {
            obj = ((Map<String, Object>) obj).get(key);
        }

        return obj;
    }

    public YamlConfiguration subconfig(String path) {
        return new YamlConfiguration(getMap(path));
    }

    public Map<String, Object> getMap(String path) {
        return (Map<String, Object>) this.get(path);
    }

    public JsonOptions getJsonOptions(String path) {
        return Optional.ofNullable(get(path))
            .map(Jackson::toJsonString)
            .map(JsonOptions::new)
            .orElseGet(JsonOptions::empty);
    }

    public String getString(String path) {
        return (String) this.get(path);
    }

    public Integer getInteger(String path) {
        return (Integer) this.get(path);
    }

    public Double getDouble(String path) {
        return (Double) this.get(path);
    }

    public List<String> getListString(String path) {
        return (List<String>) this.get(path);
    }

    public List<Integer> getListInteger(String path) {
        return (List<Integer>) this.get(path);
    }

}
