package heartwood

// Sets

type Set[T comparable] struct {
	inner map[T]struct{}
}

func NewSet[T comparable](items ...T) Set[T] {
	set := Set[T]{
		inner: make(map[T]struct{}),
	}
	for _, i := range items {
		set.Add(i)
	}
	return set
}

func (s Set[T]) Add(item T) {
	s.inner[item] = struct{}{}
}

func (s Set[T]) Remove(item T) {
	delete(s.inner, item)
}

func (s Set[T]) Contains(item T) bool {
	_, exists := s.inner[item]
	return exists
}

func (s Set[T]) Equal(other Set[T]) bool {
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

func (s Set[T]) Length() int {
	return len(s.inner)
}

func (s Set[T]) ToArray() []T {
	array := []T{}
	for i := range s.inner {
		array = append(array, i)
	}
	return array
}
