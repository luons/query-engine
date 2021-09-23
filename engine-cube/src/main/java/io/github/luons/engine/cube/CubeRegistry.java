package io.github.luons.engine.cube;

import io.github.luons.engine.core.cube.AbstractCube;

import java.util.LinkedHashMap;
import java.util.Map;

public class CubeRegistry {

    private static final Map<String, AbstractCube> CUBE_MAP = new LinkedHashMap<>();

    public static void registry(String id, AbstractCube cube) {
        CUBE_MAP.put(id, cube);
    }

    public static AbstractCube get(String id) {
        return CUBE_MAP.get(id);
    }
}
