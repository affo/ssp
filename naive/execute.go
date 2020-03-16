package naive

import (
	"context"
)

func Execute(ctx context.Context) error {
	g := GetGraph(ctx)
	roots := g.Roots()
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
