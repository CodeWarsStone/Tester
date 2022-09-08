package Tester

type ResultTester struct {
	Result   bool     `json:"r"`
	Tests    []Test   `json:"t"`
	Messages []string `json:"m"`
	Error    string   `json:"e"`
}

type Test struct {
	ClassName  string `json:"c"`
	MethodName string `json:"m"`
	Status     string `json:"s"`
	Time       string `json:"t"`
}
