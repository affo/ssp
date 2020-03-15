package naive

import (
	"context"
)

func Execute(ctx context.Context) error {
	g := GetGraph(ctx)
	roots := make([]Node, 0, len(g.Roots()))
	for _, r := range g.Roots() {
		roots = append(roots, r)
	}
	for len(roots) > 0 {
		next := make([]Node, 0)
		for _, root := range roots {
			s := root.Do()
			// TODO(affo): should broadcast.
			//  This way the stream gets consumed.
			for _, a := range g.Adjacents(root) {
				n := a.To()
				n.In(s)
				next = append(next, n)
			}
		}
		roots = next
	}
	return nil
}
