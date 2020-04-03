package ssp

func (a *Arch) KeyBy(ks KeySelector) *Arch {
	a.ks = ks
	return a
}
