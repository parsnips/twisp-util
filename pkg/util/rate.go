package util

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
)

type LocalResolver struct {
	URL string
}

func (r *LocalResolver) ResolveEndpoint(service, region string) (aws.Endpoint, error) {
	return aws.Endpoint{
		URL:           r.URL,
		SigningRegion: region,
	}, nil
}

type Retryer struct {
	base aws.Retryer
}

func (r *Retryer) IsErrorRetryable(err error) bool {
	return r.base.IsErrorRetryable(err)
}

func (r *Retryer) MaxAttempts() int {
	return 16
}

func (r *Retryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	return r.base.RetryDelay(attempt, opErr)
}

func (r *Retryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return r.base.GetRetryToken(ctx, opErr)
}

func (r *Retryer) GetInitialToken() (releaseToken func(error) error) {
	return r.base.GetInitialToken()
}

func NewRetryer() aws.Retryer {
	return &Retryer{
		base: retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 5
			o.RateLimiter = NoOpRateLimit{}
		}),
	}
}

type NoOpRateLimit struct{}

func (NoOpRateLimit) AddTokens(uint) error { return nil }
func (NoOpRateLimit) GetToken(context.Context, uint) (func() error, error) {
	return noOpToken, nil
}
func noOpToken() error { return nil }
