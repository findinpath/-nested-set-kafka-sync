package com.findinpath.sink.model;

public class NestedSetNodeLog {
    private int id;
    private NestedSetNode nestedSetNode;

    public NestedSetNodeLog(int id, NestedSetNode nestedSetNode) {
        this.id = id;
        this.nestedSetNode = nestedSetNode;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public NestedSetNode getNestedSetNode() {
        return nestedSetNode;
    }

    public void setNestedSetNode(NestedSetNode nestedSetNode) {
        this.nestedSetNode = nestedSetNode;
    }

    @Override
    public String toString() {
        return "NestedSetNodeLog{" +
                "id=" + id +
                ", nestedSetNode=" + nestedSetNode +
                '}';
    }
}
