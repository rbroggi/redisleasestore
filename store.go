package redisleasestore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	le "github.com/rbroggi/leaderelection"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	client   redis.UniversalClient
	leaseKey string
	// Pre-compiled Lua script for CreateLease
	createScript *redis.Script
}

type Args struct {
	Client   redis.UniversalClient
	LeaseKey string
}

func NewStore(args Args) (*Store, error) {
	s := &Store{
		client:       args.Client,
		leaseKey:     args.LeaseKey,
		createScript: redis.NewScript(createLease),
	}

	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	if err := s.preloadScripts(ctx); err != nil {
		return nil, fmt.Errorf("failed to preload scripts: %w", err)
	}
	return s, nil
}

func (s *Store) GetLease(ctx context.Context) (*le.Lease, error) {
	val, err := s.client.HGetAll(ctx, s.leaseKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("lease not found: %w", le.ErrLeaseNotFound)
		}
		return nil, fmt.Errorf("redis HGetAll error: %w", err)
	}

	if len(val) == 0 {
		return &le.Lease{}, nil // Lease exists but is empty
	}

	return s.valToLease(val)
}

func (s *Store) UpdateLease(ctx context.Context, newLease *le.Lease) error {
	ctx, cnl := context.WithTimeout(ctx, operationTimeout)
	defer cnl()
	_, err := s.client.HSet(ctx,
		s.leaseKey,
		"HolderIdentity", newLease.HolderIdentity,
		"AcquireTime", newLease.AcquireTime.Format(time.RFC3339Nano),
		"RenewTime", newLease.RenewTime.Format(time.RFC3339Nano),
		"LeaseDuration", newLease.LeaseDuration.String(),
		"LeaderTransitions", strconv.FormatUint(uint64(newLease.LeaderTransitions), 10),
	).Result()
	if err != nil {
		return fmt.Errorf("redis HSet error: %w", err)
	}
	return nil
}

func (s *Store) CreateLease(ctx context.Context, newLease *le.Lease) error {
	res, err := s.createScript.Run(ctx, s.client, []string{s.leaseKey},
		newLease.HolderIdentity,
		newLease.AcquireTime.Format(time.RFC3339Nano),
		newLease.RenewTime.Format(time.RFC3339Nano),
		newLease.LeaseDuration.String(),
		strconv.FormatUint(uint64(newLease.LeaderTransitions), 10), // Convert int to string for Lua
	).Int()

	if err != nil {
		return fmt.Errorf("redis script error: %w", err)
	}
	if res == 0 {
		return fmt.Errorf("failed to acquire lease: already exists")
	}
	return nil

}

func (s *Store) valToLease(val map[string]string) (*le.Lease, error) {
	acquireTime, err := time.Parse(time.RFC3339Nano, val["AcquireTime"])
	if err != nil {
		return nil, fmt.Errorf("error parsing AcquireTime: %w", err)
	}
	renewTime, err := time.Parse(time.RFC3339Nano, val["RenewTime"])
	if err != nil {
		return nil, fmt.Errorf("error parsing RenewTime: %w", err)
	}
	leaseDuration, err := time.ParseDuration(val["LeaseDuration"])
	if err != nil {
		return nil, fmt.Errorf("error parsing LeaseDuration: %w", err)
	}
	leaderTransitions := 0
	if _, ok := val["LeaderTransitions"]; ok {
		if _, err = fmt.Sscan(val["LeaderTransitions"], &leaderTransitions); err != nil {
			return nil, err
		}
	}

	return &le.Lease{
		HolderIdentity:    val["HolderIdentity"],
		AcquireTime:       acquireTime,
		RenewTime:         renewTime,
		LeaseDuration:     leaseDuration,
		LeaderTransitions: uint32(leaderTransitions),
	}, nil
}

func (s *Store) preloadScripts(ctx context.Context) error {
	if _, err := s.createScript.Load(ctx, s.client).Result(); err != nil {
		return fmt.Errorf("failed to load createScript: %w", err)
	}
	return nil
}

//go:embed scripts/create_lease.lua
var createLease string

var (
	operationTimeout = 5 * time.Second
)
