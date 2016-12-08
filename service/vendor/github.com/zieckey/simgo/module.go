package simgo

type Module interface {
	Initialize() error
	Uninitialize() error
}
