package com.findinpath.source.model;

import java.time.Instant;

/**
 * Models a nested set node.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Nested_set_model">Nested Set Model</a>
 */
public class NestedSetNode {
    private long id;
    private String label;

    private int left;
    private int right;
    private boolean active;
    private Instant created;
    private Instant updated;

    public NestedSetNode() {
    }

    public NestedSetNode(int id, String label, int left, int right) {
        this.id = id;
        this.label = label;
        this.left = left;
        this.right = right;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getLeft() {
        return left;
    }

    public void setLeft(int left) {
        this.left = left;
    }

    public int getRight() {
        return right;
    }

    public void setRight(int right) {
        this.right = right;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    @Override
    public String toString() {
        return "NestedSetNode{" +
                "id=" + id +
                ", label=" + label +
                ", left=" + left +
                ", right=" + right +
                ", active=" + active +
                ", created=" + created +
                ", updated=" + updated +
                '}';
    }
}