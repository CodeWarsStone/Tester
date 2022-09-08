package Utils

import (
	"log"
	"os"
)

var (
	ErrLog = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
	Log    = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
)
