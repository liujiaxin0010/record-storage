package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"recording-server/pb/master_pb"
)

type GRPCMasterClient struct {
	conn   *grpc.ClientConn
	client master_pb.SeaweedClient
}

func NewGRPCMasterClient(addrs []string) (*GRPCMasterClient, error) {
	var lastErr error
	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err == nil {
			return &GRPCMasterClient{conn: conn, client: master_pb.NewSeaweedClient(conn)}, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no master grpc address configured")
	}
	return nil, lastErr
}

func (c *GRPCMasterClient) Assign(ctx context.Context, p AssignParams) (AssignResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	resp, err := c.client.Assign(ctx, &master_pb.AssignRequest{
		Count:       p.Count,
		Collection:  p.Collection,
		Replication: p.Replication,
		Ttl:         p.TTL,
		DataCenter:  p.DataCenter,
		Rack:        p.Rack,
		DataNode:    p.DataNode,
		DiskType:    p.DiskType,
	})
	if err != nil {
		return AssignResult{}, classifyGRPCError(err)
	}
	if resp.GetError() != "" {
		return AssignResult{}, fmt.Errorf("assign failed: %s", resp.GetError())
	}
	loc := resp.GetLocation()
	if loc == nil || loc.GetUrl() == "" {
		return AssignResult{}, ErrVolumeRouteNotFound
	}
	return AssignResult{
		FID:       resp.GetFid(),
		VolumeID:  parseVolumeID(resp.GetFid()),
		VolumeURL: buildHTTPURL(loc.GetPublicUrl(), loc.GetUrl()),
		AuthToken: resp.GetAuth(),
	}, nil
}

func (c *GRPCMasterClient) LookupVolume(ctx context.Context, volumeOrFileIDs []string) (map[string][]VolumeLocation, error) {
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	resp, err := c.client.LookupVolume(ctx, &master_pb.LookupVolumeRequest{VolumeOrFileIds: volumeOrFileIDs})
	if err != nil {
		return nil, classifyGRPCError(err)
	}
	result := make(map[string][]VolumeLocation, len(resp.GetVolumeIdLocations()))
	for _, item := range resp.GetVolumeIdLocations() {
		locations := make([]VolumeLocation, 0, len(item.GetLocations()))
		for _, location := range item.GetLocations() {
			locations = append(locations, VolumeLocation{
				URL:       buildHTTPURL(location.GetPublicUrl(), location.GetUrl()),
				PublicURL: location.GetPublicUrl(),
				AuthToken: item.GetAuth(),
			})
		}
		result[item.GetVolumeOrFileId()] = locations
	}
	return result, nil
}

func (c *GRPCMasterClient) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := c.client.Ping(ctx, &master_pb.PingRequest{})
	if err != nil {
		return classifyGRPCError(err)
	}
	return nil
}

func (c *GRPCMasterClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func classifyGRPCError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	if st.Code() == codes.NotFound {
		return ErrNotFound
	}
	return err
}

func buildHTTPURL(publicURL, internalURL string) string {
	target := strings.TrimSpace(publicURL)
	if target == "" {
		target = strings.TrimSpace(internalURL)
	}
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		return target
	}
	return "http://" + target
}

func parseVolumeID(fid string) string {
	parts := strings.SplitN(fid, ",", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}
