package toytsdb

type Logger interface {
	Printf(format string, v ...interface{})
}

type nopLogger struct {
}

func (l *nopLogger) Log(_ string, _ ...interface{}) {
	return
}
