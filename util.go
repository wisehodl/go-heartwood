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

func (s Set[T]) ToArray() []T {
	array := []T{}
	for i := range s.inner {
		array = append(array, i)
	}
	return array
}

// Operations

func Flatten[K comparable, V comparable](mapping map[K][]V) []V {
	var values []V
	for _, array := range mapping {
		for _, v := range array {
			values = append(values, v)
		}
	}
	return values
}
