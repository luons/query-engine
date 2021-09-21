package com.luons.engine.core.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;

@JsonDeserialize(using = FilterDeserializer.class)
public interface Filter extends Serializable {

}
