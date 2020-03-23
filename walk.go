package ssp

func Walk(g Graph, f func(Stream)) {
	roots := g.Roots()
	for len(roots) > 0 {
		next := make([]Node, 0)
		for _, root := range roots {
			for _, a := range g.Adjacents(root) {
				f(a)
				n := a.To()
				next = append(next, n)
			}
		}
		roots = next
	}
}
