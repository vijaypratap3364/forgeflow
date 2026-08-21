package workflow

import (
	"container/heap"
	"sort"
	"strings"
	"unicode"
)

type dependencyGraph struct {
	tasks      map[TaskID]TaskDefinition
	taskIDs    []TaskID
	indegree   map[TaskID]int
	dependents map[TaskID][]TaskID
}

// Validate checks the workflow's identifiers, task definitions, dependency
// references, and acyclic graph invariant.
func (definition WorkflowDefinition) Validate() error {
	_, err := definition.TopologicalOrder()
	return err
}

// IsAcyclic reports whether a structurally valid workflow contains a cycle.
// Invalid identifiers and dependency references are returned as errors because
// acyclicity is not meaningful until those structural problems are resolved.
func (definition WorkflowDefinition) IsAcyclic() (bool, error) {
	graph, err := buildDependencyGraph(definition)
	if err != nil {
		return false, err
	}

	_, acyclic := graph.topologicalOrder()
	return acyclic, nil
}

// TopologicalOrder returns the lexicographically smallest valid task ordering.
func (definition WorkflowDefinition) TopologicalOrder() ([]TaskID, error) {
	graph, err := buildDependencyGraph(definition)
	if err != nil {
		return nil, err
	}

	order, acyclic := graph.topologicalOrder()
	if !acyclic {
		return nil, &ValidationError{
			Code:       ValidationCycle,
			WorkflowID: definition.ID,
		}
	}

	return order, nil
}

// RootTasks returns tasks with no dependencies, ordered lexicographically by ID.
func (definition WorkflowDefinition) RootTasks() ([]TaskDefinition, error) {
	graph, err := buildDependencyGraph(definition)
	if err != nil {
		return nil, err
	}

	if _, acyclic := graph.topologicalOrder(); !acyclic {
		return nil, &ValidationError{
			Code:       ValidationCycle,
			WorkflowID: definition.ID,
		}
	}

	var roots []TaskDefinition
	for _, taskID := range graph.taskIDs {
		if graph.indegree[taskID] == 0 {
			roots = append(roots, graph.tasks[taskID])
		}
	}

	return roots, nil
}

// ReadyTasks returns incomplete tasks whose dependencies are all present in
// completed. Results are ordered lexicographically by task ID.
func (definition WorkflowDefinition) ReadyTasks(completed map[TaskID]struct{}) ([]TaskDefinition, error) {
	graph, err := buildDependencyGraph(definition)
	if err != nil {
		return nil, err
	}

	if _, acyclic := graph.topologicalOrder(); !acyclic {
		return nil, &ValidationError{
			Code:       ValidationCycle,
			WorkflowID: definition.ID,
		}
	}

	unknownCompleted := make([]TaskID, 0)
	for taskID := range completed {
		if _, exists := graph.tasks[taskID]; !exists {
			unknownCompleted = append(unknownCompleted, taskID)
		}
	}
	sortTaskIDs(unknownCompleted)
	if len(unknownCompleted) > 0 {
		return nil, &ValidationError{
			Code:       ValidationUnknownCompletedTask,
			WorkflowID: definition.ID,
			TaskID:     unknownCompleted[0],
		}
	}

	var ready []TaskDefinition
	for _, taskID := range graph.taskIDs {
		if _, isCompleted := completed[taskID]; isCompleted {
			continue
		}

		task := graph.tasks[taskID]
		dependenciesSatisfied := true
		for _, dependencyID := range task.Dependencies {
			if _, isCompleted := completed[dependencyID]; !isCompleted {
				dependenciesSatisfied = false
				break
			}
		}
		if dependenciesSatisfied {
			ready = append(ready, task)
		}
	}

	return ready, nil
}

func buildDependencyGraph(definition WorkflowDefinition) (*dependencyGraph, error) {
	if !validIdentifier(string(definition.ID)) {
		return nil, &ValidationError{
			Code:       ValidationInvalidWorkflowID,
			WorkflowID: definition.ID,
		}
	}
	if len(definition.Tasks) == 0 {
		return nil, &ValidationError{
			Code:       ValidationEmptyWorkflow,
			WorkflowID: definition.ID,
		}
	}

	graph := &dependencyGraph{
		tasks:      make(map[TaskID]TaskDefinition, len(definition.Tasks)),
		taskIDs:    make([]TaskID, 0, len(definition.Tasks)),
		indegree:   make(map[TaskID]int, len(definition.Tasks)),
		dependents: make(map[TaskID][]TaskID, len(definition.Tasks)),
	}

	for _, task := range definition.Tasks {
		if !validIdentifier(string(task.ID)) {
			return nil, &ValidationError{
				Code:       ValidationInvalidTaskID,
				WorkflowID: definition.ID,
				TaskID:     task.ID,
			}
		}
		if strings.TrimSpace(task.Name) == "" {
			return nil, &ValidationError{
				Code:       ValidationInvalidTaskName,
				WorkflowID: definition.ID,
				TaskID:     task.ID,
			}
		}
		if _, exists := graph.tasks[task.ID]; exists {
			return nil, &ValidationError{
				Code:       ValidationDuplicateTaskID,
				WorkflowID: definition.ID,
				TaskID:     task.ID,
			}
		}

		graph.tasks[task.ID] = task
		graph.taskIDs = append(graph.taskIDs, task.ID)
		graph.indegree[task.ID] = 0
	}
	sortTaskIDs(graph.taskIDs)

	for _, taskID := range graph.taskIDs {
		task := graph.tasks[taskID]
		seenDependencies := make(map[TaskID]struct{}, len(task.Dependencies))

		for _, dependencyID := range task.Dependencies {
			if !validIdentifier(string(dependencyID)) {
				return nil, &ValidationError{
					Code:         ValidationInvalidDependencyID,
					WorkflowID:   definition.ID,
					TaskID:       task.ID,
					DependencyID: dependencyID,
				}
			}
			if dependencyID == task.ID {
				return nil, &ValidationError{
					Code:         ValidationSelfDependency,
					WorkflowID:   definition.ID,
					TaskID:       task.ID,
					DependencyID: dependencyID,
				}
			}
			if _, exists := seenDependencies[dependencyID]; exists {
				return nil, &ValidationError{
					Code:         ValidationDuplicateDependency,
					WorkflowID:   definition.ID,
					TaskID:       task.ID,
					DependencyID: dependencyID,
				}
			}
			seenDependencies[dependencyID] = struct{}{}

			if _, exists := graph.tasks[dependencyID]; !exists {
				return nil, &ValidationError{
					Code:         ValidationMissingDependency,
					WorkflowID:   definition.ID,
					TaskID:       task.ID,
					DependencyID: dependencyID,
				}
			}

			graph.indegree[task.ID]++
			graph.dependents[dependencyID] = append(graph.dependents[dependencyID], task.ID)
		}
	}

	for taskID := range graph.dependents {
		sortTaskIDs(graph.dependents[taskID])
	}

	return graph, nil
}

func (graph *dependencyGraph) topologicalOrder() ([]TaskID, bool) {
	remainingIndegree := make(map[TaskID]int, len(graph.indegree))
	ready := make(taskIDHeap, 0, len(graph.taskIDs))
	for _, taskID := range graph.taskIDs {
		remainingIndegree[taskID] = graph.indegree[taskID]
		if graph.indegree[taskID] == 0 {
			ready = append(ready, taskID)
		}
	}
	heap.Init(&ready)

	order := make([]TaskID, 0, len(graph.taskIDs))
	for ready.Len() > 0 {
		taskID := heap.Pop(&ready).(TaskID)
		order = append(order, taskID)

		for _, dependentID := range graph.dependents[taskID] {
			remainingIndegree[dependentID]--
			if remainingIndegree[dependentID] == 0 {
				heap.Push(&ready, dependentID)
			}
		}
	}

	return order, len(order) == len(graph.taskIDs)
}

func validIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}

	for _, character := range identifier {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}

	return true
}

func sortTaskIDs(taskIDs []TaskID) {
	sort.Slice(taskIDs, func(left, right int) bool {
		return taskIDs[left] < taskIDs[right]
	})
}

type taskIDHeap []TaskID

func (taskIDs taskIDHeap) Len() int {
	return len(taskIDs)
}

func (taskIDs taskIDHeap) Less(left, right int) bool {
	return taskIDs[left] < taskIDs[right]
}

func (taskIDs taskIDHeap) Swap(left, right int) {
	taskIDs[left], taskIDs[right] = taskIDs[right], taskIDs[left]
}

func (taskIDs *taskIDHeap) Push(value any) {
	*taskIDs = append(*taskIDs, value.(TaskID))
}

func (taskIDs *taskIDHeap) Pop() any {
	previous := *taskIDs
	lastIndex := len(previous) - 1
	value := previous[lastIndex]
	*taskIDs = previous[:lastIndex]
	return value
}
