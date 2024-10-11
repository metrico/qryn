package main

import "math/rand"

func pickRandom[T any](arr []T, rnd *rand.Rand) T {
	return arr[rnd.Intn(len(arr))]
}
