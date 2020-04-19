package com.findinpath.sink.service;

import com.findinpath.sink.model.NestedSetNode;
import com.findinpath.sink.model.TreeNode;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Comparators.isInStrictOrder;

class TreeUtils {

    public static Optional<TreeNode> buildTree(List<NestedSetNode> nestedSetNodes) {
        if (!isValidNestedSet(nestedSetNodes)) return Optional.empty();

        var nestedSetNodeIterator = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getLeft))
                .iterator();
        NestedSetNode rootNestedSetNode = nestedSetNodeIterator.next();
        TreeNode root = new TreeNode(rootNestedSetNode);
        Stack<TreeNode> stack = new Stack<>();
        stack.push(root);
        while (nestedSetNodeIterator.hasNext()) {
            NestedSetNode nestedSetNode = nestedSetNodeIterator.next();

            if (stack.isEmpty()) return Optional.empty();
            // find the corresponding parent node
            while (stack.peek().getNestedSetNode().getRight() < nestedSetNode.getRight()) {
                if (stack.isEmpty()) return Optional.empty();
                stack.pop();
            }
            if (stack.isEmpty()) return Optional.empty();
            TreeNode parent = stack.peek();

            TreeNode child = parent.addChild(nestedSetNode);
            stack.push(child);
        }
        return Optional.of(root);
    }

    private static boolean isValidNestedSet(List<NestedSetNode> nestedSetNodes) {
        if (nestedSetNodes == null || nestedSetNodes.isEmpty()) return false;

        var leftCoordinatesSorted = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getLeft))
                .map(NestedSetNode::getLeft)
                .collect(Collectors.toList());
        // preordered representation of the nested set should be strictly ordered
        if (!isInStrictOrder(leftCoordinatesSorted, Ordering.natural())) {
            return false;
        }

        var rightCoordinatesSorted = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getRight).reversed())
                .map(NestedSetNode::getRight)
                .collect(Collectors.toList());
        // postordered representation of the nested set should be strictly ordered
        if (!isInStrictOrder(rightCoordinatesSorted, Ordering.natural().reversed())) {
            return false;
        }
        var allCoordinates = Stream.of(leftCoordinatesSorted, rightCoordinatesSorted)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());
        // verify that there are no duplicated coordinates in the nested set
        if (!isInStrictOrder(allCoordinates, Ordering.natural())) {
            return false;
        }

        // the maximum value for a coordinate must correspond to the double of the number of nodes
        return allCoordinates.get(allCoordinates.size() - 1) == nestedSetNodes.size() * 2;
    }

    public static List<NestedSetNode> getNestedSetNodes(TreeNode root) {
        Stack<TreeNode> stack = new Stack<>();
        List<NestedSetNode> result = new ArrayList<>();
        stack.push(root);

        while (!stack.empty()) {
            var treeNode = stack.peek();
            result.add(treeNode.getNestedSetNode());
            stack.pop();

            if (treeNode.getChildren() != null) {
                var children = treeNode.getChildren();
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }
            }
        }

        return result;
    }

    public static Optional<TreeNode> applyUpdates(List<NestedSetNode> currentNestedSetNodes,
                                                  List<NestedSetNode> updatedNestedSetNodes) {
        var nestedSetNodesMap = currentNestedSetNodes.stream()
                .collect(Collectors.toMap(NestedSetNode::getId, Function.identity()));

        // apply updates
        updatedNestedSetNodes
                .forEach(nestedSetNode -> nestedSetNodesMap.put(nestedSetNode.getId(), nestedSetNode));

        return buildTree(new ArrayList<>(nestedSetNodesMap.values()));
    }

}
