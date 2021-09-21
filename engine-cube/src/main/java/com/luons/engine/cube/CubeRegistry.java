package com.luons.engine.cube;

import com.ninebot.bigdata.query.core.common.AbstractCube;

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
