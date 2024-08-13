package util

import "reflect"

func Map[A, B any](in []A, fn func(val A, index int) B) []B {
	out := make([]B, len(in))
	for i, v := range in {
		out[i] = fn(v, i)
	}
	return out
}

func MapOrError[A, B any](in []A, fn func(A, int) (B, error)) (out []B, err error) {
	out = make([]B, len(in))
	for i, a := range in {
		if out[i], err = fn(a, i); err != nil {
			return
		}
	}
	return
}

func Filter[A any](in []A, fn func(val A, index int) bool) []A {
	out := []A{}
	for i, v := range in {
		if fn(v, i) {
			out = append(out, v)
		}
	}
	return out
}

func Reduce[A, B any](in []A, fn func(result B, current A, index int) B, initial B) (r B) {
	r = initial
	for i, v := range in {
		r = fn(r, v, i)
	}
	return
}

func Keys[A comparable, B any](in map[A]B) []A {
	out := make([]A, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	return out
}

func Values[A comparable, B any](in map[A]B) []B {
	out := make([]B, 0, len(in))
	for _, v := range in {
		out = append(out, v)
	}
	return out
}

type Entry[A comparable, B any] struct {
	Key   A `json:"key"`
	Value B `json:"value"`
}

func Entries[A comparable, B any](in map[A]B) []Entry[A, B] {
	out := make([]Entry[A, B], 0, len(in))
	for k, v := range in {
		out = append(out, Entry[A, B]{k, v})
	}
	return out
}

func Reverse[A any](in []A) []A {
	out := make([]A, len(in))
	copy(out, in)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func Flatten[A any](in [][]A) []A {
	out := []A{}
	for _, list := range in {
		out = append(out, list...)
	}
	return out
}

func Concat[A any](lists ...[]A) []A {
	return Flatten(lists)
}

func Exists[A any](in []A, fn func(val A, index int) bool) bool {
	for i, v := range in {
		if fn(v, i) {
			return true
		}
	}

	return false
}

func Every[A any](in []A, fn func(val A, index int) bool) bool {
	for i, v := range in {
		if !fn(v, i) {
			return false
		}
	}
	return true
}

func Some[A any](in []A, fn func(val A, index int) bool) bool {
	for i, v := range in {
		if fn(v, i) {
			return true
		}
	}
	return false
}

func GroupBy[A any, B comparable](in []A, fn func(val A, index int) B) map[B][]A {
	grouped := map[B][]A{}
	for i, v := range in {
		k := fn(v, i)
		if _, ok := grouped[k]; !ok {
			grouped[k] = []A{}
		}
		grouped[k] = append(grouped[k], v)
	}
	return grouped
}

func Includes[A any](in []A, val A) bool {
	for _, v := range in {
		if reflect.DeepEqual(v, val) {
			return true
		}
	}
	return false
}

func OversizePartition[A any](list []A, size int, goOverSize func(previous, current A) bool) [][]A {
	if size <= 0 {
		size = 1
	}
	out := [][]A{}
	if len(list) == 0 {
		return out
	}

	current := []A{}
	for i, item := range list {
		if len(current) < size {
			current = append(current, item)
		} else if goOverSize(list[i-1], item) {
			current = append(current, item)
		} else {
			out = append(out, current)
			current = []A{item}
		}
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out
}

func Partition[A any](list []A, size int) [][]A {
	s := 1
	if size > 0 {
		s = size
	}
	if len(list) == 0 {
		return [][]A{}
	}
	out := [][]A{{}}
	c := 0
	for _, i := range list {
		if len(out[c]) >= s {
			c += 1
			out = append(out, []A{})
		}
		out[c] = append(out[c], i)
	}
	return out
}

func Range(begin, end, increment int) []int {
	if increment == 0 {
		return []int{}
	}
	if begin > end && increment > 0 {
		return []int{}
	}
	if begin < end && increment < 0 {
		return []int{}
	}
	if begin == end {
		return []int{begin}
	}
	i := begin
	out := []int{}
	if begin < end {
		for i <= end {
			out = append(out, i)
			i += increment
		}
	} else {
		for end <= i {
			out = append(out, i)
			i += increment
		}
	}
	return out
}

func Unique[A comparable](l []A) []A {
	s := map[A]bool{}
	out := []A{}
	for _, v := range l {
		if !s[v] {
			s[v] = true
			out = append(out, v)
		}
	}
	return out
}

func UniqueBy[A, B comparable](l []A, by func(A, int) B) []A {
	s := map[B]bool{}
	out := []A{}

	for i, v := range l {
		b := by(v, i)
		if _, ok := s[b]; !ok {
			s[b] = true
			out = append(out, v)
		}
	}

	return out
}

// Permutations generates all permutations of the input slice of any type.
func Permutations[T any](vals []T) [][]T {
	var result [][]T
	permutationsHelper(vals, 0, &result)
	return result
}

// permutationsHelper is a recursive helper function that generates permutationsHelper.
func permutationsHelper[T any](vals []T, start int, result *[][]T) {
	if start == len(vals)-1 {
		// Create a copy of nums and append it to result
		temp := make([]T, len(vals))
		copy(temp, vals)
		*result = append(*result, temp)
		return
	}

	for i := start; i < len(vals); i++ {
		// Swap the current element with the start element
		vals[start], vals[i] = vals[i], vals[start]
		// Recurse on the next part of the slice
		permutationsHelper(vals, start+1, result)
		// Swap back to restore the original order
		vals[start], vals[i] = vals[i], vals[start]
	}
}
