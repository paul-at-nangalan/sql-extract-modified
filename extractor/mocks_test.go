package extractor

import (
	"testing"
	"time"
)

type MockLastmodmodel struct{
	lastreadtime time.Time
}

func (p *MockLastmodmodel) Get() time.Time {
	return p.lastreadtime
}

func (p *MockLastmodmodel) Set(lastreadtime time.Time) {
	p.lastreadtime = lastreadtime
}

type MockWriter struct{
	expect [][]string
	t *testing.T
}

func (p *MockWriter) Write(actual []string) error {
	if len(p.expect) == 0{
		p.t.Error("Received extra data ", actual)
	}
	row := p.expect[0]
	for i, text := range row{
		if text != actual[i]{
			p.t.Error("Incorrect value in writer,",
				text,
				actual[i])
		}
	}
	p.expect = p.expect[1:]
	return nil
}

func (p *MockWriter)Flush(){

}

func NewMockWriter(t *testing.T)*MockWriter{
	expect := make([][]string, 0)
	return &MockWriter{
		expect: expect,
		t: t,
	}
}

