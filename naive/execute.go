package naive

import (
	"context"

	"github.com/affo/ssp"
)

func Execute(ctx context.Context) error {
	g := ssp.GetGraph(ctx)
	roots := make([]Node, 0, len(g.Roots()))
	for _, r := range g.Roots() {
		roots = append(roots, r.(Node))
	}
	for len(roots) > 0 {
		next := make([]Node, 0)
		for _, root := range roots {
			s := root.(Node).Do()
			// TODO(affo): should broadcast.
			//  This way the stream gets consumed.
			for _, a := range g.Adjacents(root) {
				n := a.To().(Node)
				n.In(s)
				next = append(next, n)
			}
		}
		roots = next
	}
	return nil
}
