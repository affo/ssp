package ssp

func in(a []Node, n Node) bool {
	for _, na := range a {
		if na == n {
			return true
		}
	}
	return false
}

func Walk(g Graph, f Visitor) {
	// Here, we could use maps, but we need slices.
	// We want a deterministic range, for a deterministic walk.
	roots := g.Roots()
	for len(roots) > 0 {
		next := make([]Node, 0)
		for _, root := range roots {
			for _, a := range g.Adjacents(root) {
				f(a)
				n := a.To()
				// Before adding this node to the next layer, check if we have already added it.
				// This is the situation that happens when 2 or more nodes point to the same one.
				if !in(next, n) {
					next = append(next, n)
				}
			}
		}
		roots = next
	}
}
