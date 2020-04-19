package com.findinpath.source.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Models a tree node that contains nested node set information.
 *
 */
public class TreeNode {
    private NestedSetNode nestedSetNode;
    private List<TreeNode> children;

    public TreeNode(NestedSetNode nestedSetNode) {
        this.nestedSetNode = nestedSetNode;
    }

    public TreeNode(NestedSetNode nestedSetNode, List<TreeNode> children) {
        this(nestedSetNode);
        this.children = children == null ? null : new ArrayList<>(children);
    }

    public NestedSetNode getNestedSetNode() {
        return nestedSetNode;
    }

    public void setNestedSetNode(NestedSetNode nestedSetNode) {
        this.nestedSetNode = nestedSetNode;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }

    public TreeNode addChild(NestedSetNode nestedSetNode) {
        if (children == null) {
            children = new ArrayList<>();
        }
        TreeNode child = new TreeNode(nestedSetNode);
        children.add(child);
        return child;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder(50);
        print(buffer, "", "");
        return buffer.toString();
    }

    private void print(StringBuilder buffer, String prefix, String childrenPrefix) {
        buffer.append(prefix);
        buffer.append("|" + nestedSetNode.getLeft() + "| " + nestedSetNode.getLabel() + " |" + nestedSetNode.getRight() + "|");
        buffer.append('\n');
        if (children != null && !children.isEmpty()) {
            var nodeLeftDigitsCount = Integer.toString(nestedSetNode.getLeft()).length();
            var leftPad = String.format("%1$" + (nodeLeftDigitsCount + 3) + "s", "");
            for (Iterator<TreeNode> it = children.iterator(); it.hasNext(); ) {
                TreeNode next = it.next();
                if (it.hasNext()) {
                    next.print(buffer,
                            childrenPrefix + leftPad + "├── ",
                            childrenPrefix + leftPad + "│   ");
                } else {
                    next.print(buffer,
                            childrenPrefix + leftPad + "└── ",
                            childrenPrefix + leftPad + "    ");
                }
            }
        }
    }
}
