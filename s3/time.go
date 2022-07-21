package s3

import "time"

type TimeSource interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type TimeSourceAdvancer interface {
	TimeSource
	Advance(by time.Duration)
}

func FixedTimeSource(at time.Time) TimeSourceAdvancer {
	return &fixedTimeSource{time: at}
}

func DefaultTimeSource() TimeSource {
	return &locatedTimeSource{
		timeLocation: time.FixedZone("GMT", 0),
	}
}

type locatedTimeSource struct {
	timeLocation *time.Location
}

func (l *locatedTimeSource) Now() time.Time {
	return time.Now().In(l.timeLocation)
}

func (l *locatedTimeSource) Since(t time.Time) time.Duration {
	return time.Since(t)
}

type fixedTimeSource struct {
	time time.Time
}

func (l *fixedTimeSource) Now() time.Time {
	return l.time
}

func (l *fixedTimeSource) Since(t time.Time) time.Duration {
	return l.time.Sub(t)
}

func (l *fixedTimeSource) Advance(by time.Duration) {
	l.time = l.time.Add(by)
}
