package com.luons.engine.core.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.luons.engine.core.enums.Connector;

import java.util.LinkedList;
import java.util.List;

@JsonDeserialize()
public class FilterGroup implements Filter {

    private Connector connector;

    private List<Filter> filterList = new LinkedList<>();

    public FilterGroup() {
        this.connector = Connector.AND;
    }

    public FilterGroup(Connector connector) {
        this.connector = connector;
    }

    public FilterGroup addFilter(Filter filter) {
        if (filter != null) {
            filterList.add(filter);
        }
        return this;
    }

    public Connector getConnector() {
        return connector;
    }

    public List<Filter> getFilterList() {
        return filterList;
    }

    private FilterGroup append(Connector connector, Filter filter) {
        if (connector.equals(this.getConnector())) {
            this.getFilterList().add(filter);
            return this;
        } else {
            FilterGroup re = new FilterGroup(connector);
            re.getFilterList().add(this);
            re.getFilterList().add(filter);
            return re;
        }
    }

    public FilterGroup and(Filter filter) {
        return append(Connector.AND, filter);
    }

    public FilterGroup or(Filter filter) {
        return append(Connector.OR, filter);
    }
}
