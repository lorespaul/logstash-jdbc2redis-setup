package com.lorenzodaneo.messagebroker;

import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.stream.Collectors;

public class Assignments {

    @Getter
    @Setter
    private Map<String, List<Integer>> assignmentsByConsumer = new HashMap<>();

    public Map<String, List<Integer>> getAssignmentsOfGroup(String group){
        return getAssignmentsByConsumer()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(group))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));
    }

    public Optional<Map.Entry<String, List<Integer>>> getAssignmentWithMinSizeOfGroup(String group){
        return getAssignmentsOfGroup(group)
                .entrySet()
                .stream()
                .min((x, y) -> {
                    if(x.getValue().size() > y.getValue().size())
                        return 1;
                    else if(x.getValue().size() < y.getValue().size())
                        return -1;
                    return 0;
                })
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new ArrayList<>(e.getValue())));
    }

    public Optional<Map.Entry<String, List<Integer>>> getAssignmentWithMaxSizeOfGroup(String group){
        return getAssignmentsOfGroup(group)
                .entrySet()
                .stream()
                .max((x, y) -> {
                    if(x.getValue().size() > y.getValue().size())
                        return 1;
                    else if(x.getValue().size() < y.getValue().size())
                        return -1;
                    return 0;
                })
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new ArrayList<>(e.getValue())));
    }

    public void removeAssignmentFromConsumer(String consumer, int index){
        List<Integer> newAssignments = new ArrayList<>(getAssignmentsByConsumer().get(consumer));
        newAssignments.remove(index);
        getAssignmentsByConsumer().put(consumer, newAssignments);
    }

    public void addAssignmentToConsumer(String consumer, int value){
        List<Integer> newAssignments = new ArrayList<>(getAssignmentsByConsumer().get(consumer));
        newAssignments.add(value);
        getAssignmentsByConsumer().put(consumer, newAssignments);
    }

    public void putConsumerAssignments(String consumer, List<Integer> values){
        getAssignmentsByConsumer().put(consumer, values);
    }

    public void removeConsumerAssignment(String consumer){
        getAssignmentsByConsumer().remove(consumer);
    }

}