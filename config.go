package code

type Config struct {
	Handler HandlerConfig   `mapstructure:"handler"`
	Loader  StructureConfig `mapstructure:"loader"`
}
