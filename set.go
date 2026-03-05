package heartwood

import (
	"sort"
)

// Sets

type StringSet struct {
	inner  map[string]struct{}
	sorted []string
}

func NewStringSet(items ...string) *StringSet {
	set := &StringSet{
		inner:  make(map[string]struct{}),
		sorted: []string{},
	}
	for _, i := range items {
		set.Add(i)
	}
	return set
}

func (s *StringSet) Add(item string) {
	if _, exists := s.inner[item]; !exists {
		s.inner[item] = struct{}{}
		s.rebuildSorted()
	}
}

func (s *StringSet) Remove(item string) {
	if _, exists := s.inner[item]; exists {
		delete(s.inner, item)
		s.rebuildSorted()
	}
}

func (s *StringSet) Contains(item string) bool {
	_, exists := s.inner[item]
	return exists
}

func (s *StringSet) Equal(other StringSet) bool {
	if len(s.inner) != len(other.inner) {
		return false
	}
	for item := range s.inner {
		if !other.Contains(item) {
			return false
		}
	}
	return true
}

func (s *StringSet) Length() int {
	return len(s.inner)
}

func (s *StringSet) AsSortedArray() []string {
	return s.sorted
}

func (s *StringSet) rebuildSorted() {
	s.sorted = make([]string, 0, len(s.inner))
	for item := range s.inner {
		s.sorted = append(s.sorted, item)
	}
	sort.Strings(s.sorted)
}
