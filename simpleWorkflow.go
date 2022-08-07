package go_workflow

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

type SimpleWorkflow struct {
	steps        *list.List
	status       Status
	dependencies map[Step][]Step
}

func sliceContainsStep(slice []Step, element Step) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}
	return false
}

// hasCycle check if the workflow has cycle starting from currentStep.
// visited contains the list of already visited steps.
// secureCycles contains the list of all steps that have already been checked and can be skipped.
func (s SimpleWorkflow) hasCycle(visited []Step, secureCycles []Step, currentStep Step) (bool, []Step) {
	if sliceContainsStep(secureCycles, currentStep) {
		// The step has already been checked and has no cycle
		return false, []Step{}
	}
	if sliceContainsStep(visited, currentStep) {
		// The step has already been visited, this is a cycle
		return true, []Step{}
	}
	visited = append(visited, currentStep)

	currentStepSecureCycles := make([]Step, 0, s.steps.Len())

	// Check for each step if its dependencies contains cycles
	if dependencies, ok := s.dependencies[currentStep]; ok {
		for _, dependency := range dependencies {
			if hasCycle, dependencySecureCycles := s.hasCycle(visited, secureCycles, dependency); !hasCycle {
				currentStepSecureCycles = append(currentStepSecureCycles, dependencySecureCycles...)
			} else {
				// A dependency has a cycle, return true and no secure steps
				return true, []Step{}
			}
		}
	}
	// this dependency is safe
	currentStepSecureCycles = append(currentStepSecureCycles, currentStep)
	return false, currentStepSecureCycles
}

func (s *SimpleWorkflow) Init(context context.Context) error {
	visitedSteps := make([]Step, 0, s.steps.Len())
	secureSteps := make([]Step, 0, s.steps.Len())

	for e := s.steps.Front(); e != nil; e = e.Next() {
		currentStep := e.Value.(Step)
		if hasCycles, stepDependency := s.hasCycle(visitedSteps, secureSteps, currentStep); !hasCycles {
			secureSteps = append(secureSteps, stepDependency...)
		} else {
			return fmt.Errorf("cycle detected")
		}
	}

	return nil
}

var stepOutputContextKey struct{}

// MakeSimpleWorkflow creates a SimpleWorkflow instance
func MakeSimpleWorkflow() *SimpleWorkflow {
	return &SimpleWorkflow{
		steps:        list.New(),
		status:       CREATED,
		dependencies: make(map[Step][]Step),
	}
}

func (s SimpleWorkflow) GetStatus() Status {
	return s.status
}

func (s *SimpleWorkflow) getNextSteps() []Step {
	nextSteps := make([]Step, 0, s.steps.Len())
	for e := s.steps.Front(); e != nil; e = e.Next() {
		step := e.Value.(Step)
		if !step.GetStatus().IsFinished() && step.GetStatus() != RUNNING && areRequirementsFullFilled(step, s.dependencies) {
			nextSteps = append(nextSteps, step)
		}
	}
	return nextSteps
}

func (s *SimpleWorkflow) executeStep(ctx context.Context, step Step, stdout *os.File) {
	requirementsOutputs := s.getRequirementsOutputs(step)
	stepContext := context.WithValue(ctx, stepOutputContextKey, requirementsOutputs)
	_, err := step.Execute(stepContext, stdout)
	if err != nil {
		log.Printf("[WARN]: step %v ended with error : %v", step.GetId(), err)
	}
}

func (s *SimpleWorkflow) cancelNextSteps(lastStep Step) error {
	errorsLst := createErrorList(s.steps.Len())

	concernedSteps := getStepThatRequires(lastStep, s.dependencies)

	for _, step := range concernedSteps {
		if step.GetStatus().IsCancellable() {
			err := step.Cancel()
			if err != nil {
				errorsLst = append(errorsLst, fmt.Errorf("fail to cancel step %v : %v", step.GetId(), err))
			}
		}
	}
	if len(errorsLst) != 0 {
		return joinErrorList(errorsLst)
	}
	return nil
}

func (s *SimpleWorkflow) getRequirementsOutputs(step Step) map[string]map[string]string {
	res := make(map[string]map[string]string)
	stepDependencies := s.dependencies[step]

	for _, dependency := range stepDependencies {
		res[dependency.GetId()] = dependency.GetOutput()
		res = mergeStringStringStringMaps(res, s.getRequirementsOutputs(dependency))
	}

	return res
}

func (s *SimpleWorkflow) getOutput() map[string]map[string]string {
	output := make(map[string]map[string]string)
	for e := s.steps.Front(); e != nil; e = e.Next() {
		step := e.Value.(Step)
		output[step.GetId()] = step.GetOutput()
	}
	return output
}

func (s *SimpleWorkflow) hasUnfinishedSteps() bool {
	for e := s.steps.Front(); e != nil; e = e.Next() {
		step := e.Value.(Step)
		if !step.GetStatus().IsFinished() {
			return true
		}
	}
	return false
}

func (s *SimpleWorkflow) Execute(ctx context.Context, stdout *os.File) (map[string]map[string]string, error) {
	closingSteps := make(chan Step, s.steps.Len())
	errorLst := make([]error, 0, s.steps.Len())

	activeSteps := &sync.WaitGroup{}

	s.startNextSteps(ctx, stdout, activeSteps, closingSteps)

	for s.hasUnfinishedSteps() {
		select {
		case <-ctx.Done():
			errorLst = s.cancelRemainingSteps(errorLst)
			close(closingSteps)

		case step, ok := <-closingSteps:
			if ok {

			}
			log.Printf("[INFO] : Step %v terminated with status %v", step.GetId(), step.GetStatus().GetName())
			// A step as ended
			if step.GetStatus() == ERROR || step.GetStatus() == CANCELLED {
				errorLst = appendErrorList(errorLst, s.cancelNextSteps(step))
			} else {
				s.startNextSteps(ctx, stdout, activeSteps, closingSteps)
			}
		}
	}

	if len(errorLst) > 0 {
		return nil, joinErrorList(errorLst)
	}

	activeSteps.Wait()

	return s.getOutput(), nil
}

func (s *SimpleWorkflow) cancelRemainingSteps(errorLst []error) []error {
	for e := s.steps.Front(); e != nil; e = e.Next() {
		step := e.Value.(Step)
		if step.GetStatus().IsCancellable() {
			err := step.Cancel()
			if err != nil {
				errorLst = append(errorLst, err)
			}
		}
	}
	return errorLst
}

func (s *SimpleWorkflow) startNextSteps(ctx context.Context, stdout *os.File, activeSteps *sync.WaitGroup, closingSteps chan Step) {
	nextSteps := s.getNextSteps()
	for _, step := range nextSteps {
		activeSteps.Add(1)

		go func(step Step) {
			s.executeStep(ctx, step, stdout)
			closingSteps <- step
			activeSteps.Done()
		}(step)
	}
}

func (s *SimpleWorkflow) debug() {
	for e := s.steps.Front(); e != nil; e = e.Next() {
		step := e.Value.(Step)
		log.Printf("[DEBUG]: %v : %v", step.GetId(), step.GetStatus().GetName())
	}
}

func (s *SimpleWorkflow) AddStep(step Step, dependencies []Step) error {
	for e := s.steps.Front(); e != nil; e = e.Next() {
		elementAsStep, ok := e.Value.(Step)
		if !ok {
			return fmt.Errorf("workflow is invalid")
		}

		if elementAsStep.GetId() == step.GetId() {
			return fmt.Errorf("step [%s] is already present in workflow", step.GetId())
		}
	}

	s.steps.PushBack(step)
	s.dependencies[step] = dependencies
	return nil
}

func getStepThatRequires(requiredStep Step, dependencies map[Step][]Step) []Step {
	results := make([]Step, 0, len(dependencies))
	for step, stepDependencies := range dependencies {
		for _, dependency := range stepDependencies {
			if dependency == requiredStep {
				results = append(results, step)
			}
			// Switch to next step
			break
		}
	}
	return results
}

func areRequirementsFullFilled(step Step, dependencies map[Step][]Step) bool {
	for _, dep := range dependencies[step] {
		if dep.GetStatus() != SUCCESS {
			return false
		}
	}
	return true
}
